/**
 * PhysicsFS; a portable, flexible file i/o abstraction.
 *
 * Documentation is in physfs.h. It's verbose, honest.  :)
 *
 * Please see the file LICENSE.txt in the source's root directory.
 *
 *  This file written by Ryan C. Gordon.
 */

#define __PHYSICSFS_INTERNAL__
#include "physfs_internal.h"


typedef struct __PHYSFS_DIRHANDLE__
{
    void *opaque;  /* Instance data unique to the archiver. */
    char *dirName;  /* Path to archive in platform-dependent notation. */
    char *mountPoint; /* Mountpoint in virtual file tree. */
    const PHYSFS_Archiver *funcs;  /* Ptr to archiver info for this handle. */
    struct __PHYSFS_DIRHANDLE__ *next;  /* linked list stuff. */
} DirHandle;


typedef struct __PHYSFS_FILEHANDLE__
{
    PHYSFS_Io *io;  /* Instance data unique to the archiver for this file. */
    PHYSFS_uint8 forReading; /* Non-zero if reading, zero if write/append */
    const DirHandle *dirHandle;  /* Archiver instance that created this */
    PHYSFS_uint8 *buffer;  /* Buffer, if set (NULL otherwise). Don't touch! */
    size_t bufsize;  /* Bufsize, if set (0 otherwise). Don't touch! */
    size_t buffill;  /* Buffer fill size. Don't touch! */
    size_t bufpos;  /* Buffer position. Don't touch! */
    struct __PHYSFS_FILEHANDLE__ *next;  /* linked list stuff. */
} FileHandle;


typedef struct __PHYSFS_ERRSTATETYPE__
{
    void *tid;
    PHYSFS_ErrorCode code;
    struct __PHYSFS_ERRSTATETYPE__ *next;
} ErrState;

/* Multi initialization */
static int multi_initialized = 0;

/* General PhysicsFS state ... */
static int initialized[NUM_DRIVES];
static ErrState *errorStates[NUM_DRIVES];
static DirHandle *searchPath[NUM_DRIVES];
static DirHandle *writeDir[NUM_DRIVES];
static FileHandle *openWriteList[NUM_DRIVES];
static FileHandle *openReadList[NUM_DRIVES];
static char *baseDir[NUM_DRIVES];
static char *userDir[NUM_DRIVES];
static char *prefDir[NUM_DRIVES];
static int allowSymLinks[NUM_DRIVES];
static volatile size_t numArchivers[NUM_DRIVES];

/* mutexes ... */
static void *errorLock[NUM_DRIVES];
static void *stateLock[NUM_DRIVES];

/* allocator[NUM_DRIVES] ... */
static int externalAllocator[NUM_DRIVES];
PHYSFS_Allocator allocator[NUM_DRIVES];


#ifdef PHYSFS_NEED_ATOMIC_OP_FALLBACK
static inline int __PHYSFS_atomicAdd(int *ptrval, const int val, const unsigned char dv)
{
    int retval;
    __PHYSFS_platformGrabMutex(stateLock[dv]);
    retval = *ptrval;
    *ptrval = retval + val;
    __PHYSFS_platformReleaseMutex(stateLock[dv]);
    return retval;
} /* __PHYSFS_atomicAdd */

int __PHYSFS_ATOMIC_INCR(int *ptrval, const unsigned char dv)
{
    return __PHYSFS_atomicAdd(ptrval, 1);
} /* __PHYSFS_ATOMIC_INCR */

int __PHYSFS_ATOMIC_DECR(int *ptrval, const unsigned char dv)
{
    return __PHYSFS_atomicAdd(ptrval, -1);
} /* __PHYSFS_ATOMIC_DECR */
#endif



/* PHYSFS_Io implementation for i/o to physical filesystem... */

/* !!! FIXME: maybe refcount the paths in a string pool? */
typedef struct __PHYSFS_NativeIoInfo
{
    void *handle;
    const char *path;
    int mode;   /* 'r', 'w', or 'a' */
} NativeIoInfo;

static PHYSFS_sint64 nativeIo_read(PHYSFS_Io *io, void *buf, PHYSFS_uint64 len, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformRead(info->handle, buf, len, dv);
} /* nativeIo_read */

static PHYSFS_sint64 nativeIo_write(PHYSFS_Io *io, const void *buffer,
                                    PHYSFS_uint64 len, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformWrite(info->handle, buffer, len, dv);
} /* nativeIo_write */

static int nativeIo_seek(PHYSFS_Io *io, PHYSFS_uint64 offset, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformSeek(info->handle, offset, dv);
} /* nativeIo_seek */

static PHYSFS_sint64 nativeIo_tell(PHYSFS_Io *io, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformTell(info->handle, dv);
} /* nativeIo_tell */

static PHYSFS_sint64 nativeIo_length(PHYSFS_Io *io, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformFileLength(info->handle, dv);
} /* nativeIo_length */

static PHYSFS_Io *nativeIo_duplicate(PHYSFS_Io *io, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_createNativeIo(info->path, info->mode, dv);
} /* nativeIo_duplicate */

static int nativeIo_flush(PHYSFS_Io *io, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    return __PHYSFS_platformFlush(info->handle, dv);
} /* nativeIo_flush */

static void nativeIo_destroy(PHYSFS_Io *io, const unsigned char dv)
{
    NativeIoInfo *info = (NativeIoInfo *) io->opaque;
    __PHYSFS_platformClose(info->handle,dv);
    allocator[dv].Free((void *) info->path, dv);
    allocator[dv].Free(info, dv);
    allocator[dv].Free(io, dv);
} /* nativeIo_destroy */

static const PHYSFS_Io __PHYSFS_nativeIoInterface =
{
    CURRENT_PHYSFS_IO_API_VERSION, NULL,
    nativeIo_read,
    nativeIo_write,
    nativeIo_seek,
    nativeIo_tell,
    nativeIo_length,
    nativeIo_duplicate,
    nativeIo_flush,
    nativeIo_destroy
};

PHYSFS_Io *__PHYSFS_createNativeIo(const char *path, const int mode, const unsigned char dv)
{
    PHYSFS_Io *io = NULL;
    NativeIoInfo *info = NULL;
    void *handle = NULL;
    char *pathdup = NULL;

    assert((mode == 'r') || (mode == 'w') || (mode == 'a'));

    io = (PHYSFS_Io *) allocator[dv].Malloc(sizeof (PHYSFS_Io), dv);
    GOTO_IF(!io, PHYSFS_ERR_OUT_OF_MEMORY, createNativeIo_failed, dv);
    info = (NativeIoInfo *) allocator[dv].Malloc(sizeof (NativeIoInfo), dv);
    GOTO_IF(!info, PHYSFS_ERR_OUT_OF_MEMORY, createNativeIo_failed, dv);
    pathdup = (char *) allocator[dv].Malloc(strlen(path) + 1, dv);
    GOTO_IF(!pathdup, PHYSFS_ERR_OUT_OF_MEMORY, createNativeIo_failed, dv);

    if (mode == 'r')
        handle = __PHYSFS_platformOpenRead(path,dv);
    else if (mode == 'w')
        handle = __PHYSFS_platformOpenWrite(path,dv);
    else if (mode == 'a')
        handle = __PHYSFS_platformOpenAppend(path,dv);

    GOTO_IF_ERRPASS(!handle, createNativeIo_failed, dv);

    strcpy(pathdup, path);
    info->handle = handle;
    info->path = pathdup;
    info->mode = mode;
    memcpy(io, &__PHYSFS_nativeIoInterface, sizeof (*io));
    io->opaque = info;
    return io;

createNativeIo_failed:
    if (handle != NULL) __PHYSFS_platformClose(handle,dv);
    if (pathdup != NULL) allocator[dv].Free(pathdup, dv);
    if (info != NULL) allocator[dv].Free(info, dv);
    if (io != NULL) allocator[dv].Free(io, dv);
    return NULL;
} /* __PHYSFS_createNativeIo */


/* PHYSFS_Io implementation for i/o to a PHYSFS_File... */

static PHYSFS_sint64 handleIo_read(PHYSFS_Io *io, void *buf, PHYSFS_uint64 len, const unsigned char dv)
{
    return PHYSFS_readBytes((PHYSFS_File *) io->opaque, buf, len, dv);
} /* handleIo_read */

static PHYSFS_sint64 handleIo_write(PHYSFS_Io *io, const void *buffer,
                                    PHYSFS_uint64 len, const unsigned char dv)
{
    return PHYSFS_writeBytes((PHYSFS_File *) io->opaque, buffer, len, dv);
} /* handleIo_write */

static int handleIo_seek(PHYSFS_Io *io, PHYSFS_uint64 offset, const unsigned char dv)
{
    return PHYSFS_seek((PHYSFS_File *) io->opaque, offset, dv);
} /* handleIo_seek */

static PHYSFS_sint64 handleIo_tell(PHYSFS_Io *io, const unsigned char dv)
{
    return PHYSFS_tell((PHYSFS_File *) io->opaque, dv);
} /* handleIo_tell */

static PHYSFS_sint64 handleIo_length(PHYSFS_Io *io, const unsigned char dv)
{
    return PHYSFS_fileLength((PHYSFS_File *) io->opaque, dv);
} /* handleIo_length */

static PHYSFS_Io *handleIo_duplicate(PHYSFS_Io *io, const unsigned char dv)
{
    /*
     * There's no duplicate at the PHYSFS_File level, so we break the
     *  abstraction. We're allowed to: we're physfs.c!
     */
    FileHandle *origfh = (FileHandle *) io->opaque;
    FileHandle *newfh = (FileHandle *) allocator[dv].Malloc(sizeof (FileHandle), dv);
    PHYSFS_Io *retval = NULL;

    GOTO_IF(!newfh, PHYSFS_ERR_OUT_OF_MEMORY, handleIo_dupe_failed, dv);
    memset(newfh, '\0', sizeof (*newfh));

    retval = (PHYSFS_Io *) allocator[dv].Malloc(sizeof (PHYSFS_Io), dv);
    GOTO_IF(!retval, PHYSFS_ERR_OUT_OF_MEMORY, handleIo_dupe_failed, dv);

#if 0  /* we don't buffer the duplicate, at least not at the moment. */
    if (origfh->buffer != NULL)
    {
        newfh->buffer = (PHYSFS_uint8 *) allocator[dv].Malloc(origfh->bufsize, dv);
        if (!newfh->buffer)
            GOTO(PHYSFS_ERR_OUT_OF_MEMORY, handleIo_dupe_failed);
        newfh->bufsize = origfh->bufsize;
    } /* if */
#endif

    newfh->io = origfh->io->duplicate(origfh->io, dv);
    GOTO_IF_ERRPASS(!newfh->io, handleIo_dupe_failed, dv);

    newfh->forReading = origfh->forReading;
    newfh->dirHandle = origfh->dirHandle;

    __PHYSFS_platformGrabMutex(stateLock[dv]);
    if (newfh->forReading)
    {
        newfh->next = openReadList[dv];
        openReadList[dv] = newfh;
    } /* if */
    else
    {
        newfh->next = openWriteList[dv];
        openWriteList[dv] = newfh;
    } /* else */
    __PHYSFS_platformReleaseMutex(stateLock[dv]);

    memcpy(retval, io, sizeof (PHYSFS_Io));
    retval->opaque = newfh;
    return retval;
    
handleIo_dupe_failed:
    if (newfh)
    {
        if (newfh->io != NULL) newfh->io->destroy(newfh->io, dv);
        if (newfh->buffer != NULL) allocator[dv].Free(newfh->buffer, dv);
        allocator[dv].Free(newfh, dv);
    } /* if */

    return NULL;
} /* handleIo_duplicate */

static int handleIo_flush(PHYSFS_Io *io, const unsigned char dv)
{
    return PHYSFS_flush((PHYSFS_File *) io->opaque, dv);
} /* handleIo_flush */

static void handleIo_destroy(PHYSFS_Io *io, const unsigned char dv)
{
    if (io->opaque != NULL)
        PHYSFS_close((PHYSFS_File *) io->opaque, dv);
    allocator[dv].Free(io, dv);
} /* handleIo_destroy */

static const PHYSFS_Io __PHYSFS_handleIoInterface =
{
    CURRENT_PHYSFS_IO_API_VERSION, NULL,
    handleIo_read,
    handleIo_write,
    handleIo_seek,
    handleIo_tell,
    handleIo_length,
    handleIo_duplicate,
    handleIo_flush,
    handleIo_destroy
};

static PHYSFS_Io *__PHYSFS_createHandleIo(PHYSFS_File *f, const unsigned char dv)
{
    PHYSFS_Io *io = (PHYSFS_Io *) allocator[dv].Malloc(sizeof (PHYSFS_Io), dv);
    BAIL_IF(!io, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    memcpy(io, &__PHYSFS_handleIoInterface, sizeof (*io));
    io->opaque = f;
    return io;
} /* __PHYSFS_createHandleIo */


/* functions ... */

typedef struct
{
    char **list;
    PHYSFS_uint32 size;
    PHYSFS_ErrorCode errcode;
} EnumStringListCallbackData;

static void enumStringListCallback(void *data, const char *str, const unsigned char dv)
{
    void *ptr;
    char *newstr;
    EnumStringListCallbackData *pecd = (EnumStringListCallbackData *) data;

    if (pecd->errcode)
        return;

    ptr = allocator[dv].Realloc(pecd->list, (pecd->size + 2) * sizeof (char *), dv);
    newstr = (char *) allocator[dv].Malloc(strlen(str) + 1, dv);
    if (ptr != NULL)
        pecd->list = (char **) ptr;

    if ((ptr == NULL) || (newstr == NULL))
    {
        pecd->errcode = PHYSFS_ERR_OUT_OF_MEMORY;
        pecd->list[pecd->size] = NULL;
        PHYSFS_freeList(pecd->list, dv);
        return;
    } /* if */

    strcpy(newstr, str);
    pecd->list[pecd->size] = newstr;
    pecd->size++;
} /* enumStringListCallback */


static char **doEnumStringList(void (*func)(PHYSFS_StringCallback, void *, const unsigned char dv), const unsigned char dv)
{
    EnumStringListCallbackData ecd;
    memset(&ecd, '\0', sizeof (ecd));
    ecd.list = (char **) allocator[dv].Malloc(sizeof (char *), dv);
    BAIL_IF(!ecd.list, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    func(enumStringListCallback, &ecd, dv);

    if (ecd.errcode)
    {
        PHYSFS_setErrorCode(ecd.errcode, dv);
        return NULL;
    } /* if */

    ecd.list[ecd.size] = NULL;
    return ecd.list;
} /* doEnumStringList */


static void __PHYSFS_bubble_sort(void *a, size_t lo, size_t hi,
                                 int (*cmpfn)(void *, size_t, size_t),
                                 void (*swapfn)(void *, size_t, size_t), const unsigned char dv)
{
    size_t i;
    int sorted;

    do
    {
        sorted = 1;
        for (i = lo; i < hi; i++)
        {
            if (cmpfn(a, i, i + 1) > 0)
            {
                swapfn(a, i, i + 1);
                sorted = 0;
            } /* if */
        } /* for */
    } while (!sorted);
} /* __PHYSFS_bubble_sort */


static void __PHYSFS_quick_sort(void *a, size_t lo, size_t hi,
                         int (*cmpfn)(void *, size_t, size_t),
                         void (*swapfn)(void *, size_t, size_t), const unsigned char dv)
{
    size_t i;
    size_t j;
    size_t v;

    if ((hi - lo) <= PHYSFS_QUICKSORT_THRESHOLD)
        __PHYSFS_bubble_sort(a, lo, hi, cmpfn, swapfn, dv);
    else
    {
        i = (hi + lo) / 2;

        if (cmpfn(a, lo, i) > 0) swapfn(a, lo, i);
        if (cmpfn(a, lo, hi) > 0) swapfn(a, lo, hi);
        if (cmpfn(a, i, hi) > 0) swapfn(a, i, hi);

        j = hi - 1;
        swapfn(a, i, j);
        i = lo;
        v = j;
        while (1)
        {
            while(cmpfn(a, ++i, v) < 0) { /* do nothing */ }
            while(cmpfn(a, --j, v) > 0) { /* do nothing */ }
            if (j < i)
                break;
            swapfn(a, i, j);
        } /* while */
        if (i != (hi-1))
            swapfn(a, i, hi-1);
        __PHYSFS_quick_sort(a, lo, j, cmpfn, swapfn, dv);
        __PHYSFS_quick_sort(a, i+1, hi, cmpfn, swapfn, dv);
    } /* else */
} /* __PHYSFS_quick_sort */


void __PHYSFS_sort(void *entries, size_t max,
                   int (*cmpfn)(void *, size_t, size_t),
                   void (*swapfn)(void *, size_t, size_t), const unsigned char dv)
{
    /*
     * Quicksort w/ Bubblesort fallback algorithm inspired by code from here:
     *   https://www.cs.ubc.ca/spider/harrison/Java/sorting-demo.html
     */
    if (max > 0)
        __PHYSFS_quick_sort(entries, 0, max - 1, cmpfn, swapfn, dv);
} /* __PHYSFS_sort */


static ErrState *findErrorForCurrentThread(const unsigned char dv)
{
    ErrState *i;
    void *tid;

    if (errorLock[dv] != NULL)
        __PHYSFS_platformGrabMutex(errorLock[dv]);

    if (errorStates[dv] != NULL)
    {
        tid = __PHYSFS_platformGetThreadID();

        for (i = errorStates[dv]; i != NULL; i = i->next)
        {
            if (i->tid == tid)
            {
                if (errorLock[dv] != NULL)
                    __PHYSFS_platformReleaseMutex(errorLock[dv]);
                return i;
            } /* if */
        } /* for */
    } /* if */

    if (errorLock[dv] != NULL)
        __PHYSFS_platformReleaseMutex(errorLock[dv]);

    return NULL;   /* no error available. */
} /* findErrorForCurrentThread */


/* this doesn't reset the error state. */
static inline PHYSFS_ErrorCode currentErrorCode(const unsigned char dv)
{
    const ErrState *err = findErrorForCurrentThread(dv);
    return err ? err->code : PHYSFS_ERR_OK;
} /* currentErrorCode */


PHYSFS_ErrorCode PHYSFS_getLastErrorCode(const unsigned char dv)
{
    ErrState *err = findErrorForCurrentThread(dv);
    const PHYSFS_ErrorCode retval = (err) ? err->code : PHYSFS_ERR_OK;
    if (err)
        err->code = PHYSFS_ERR_OK;
    return retval;
} /* PHYSFS_getLastErrorCode */


PHYSFS_DECL const char *PHYSFS_getErrorByCode(PHYSFS_ErrorCode code, const unsigned char dv)
{
    switch (code)
    {
        case PHYSFS_ERR_OK: return "no error";
        case PHYSFS_ERR_OTHER_ERROR: return "unknown error";
        case PHYSFS_ERR_OUT_OF_MEMORY: return "out of memory";
        case PHYSFS_ERR_NOT_INITIALIZED: return "not initialized[dv]";
        case PHYSFS_ERR_IS_INITIALIZED: return "already initialized[dv]";
        case PHYSFS_ERR_ARGV0_IS_NULL: return "argv[0] is NULL";
        case PHYSFS_ERR_UNSUPPORTED: return "unsupported";
        case PHYSFS_ERR_PAST_EOF: return "past end of file";
        case PHYSFS_ERR_FILES_STILL_OPEN: return "files still open";
        case PHYSFS_ERR_INVALID_ARGUMENT: return "invalid argument";
        case PHYSFS_ERR_NOT_MOUNTED: return "not mounted";
        case PHYSFS_ERR_NOT_FOUND: return "not found";
        case PHYSFS_ERR_SYMLINK_FORBIDDEN: return "symlinks are forbidden";
        case PHYSFS_ERR_NO_WRITE_DIR: return "write directory is not set";
        case PHYSFS_ERR_OPEN_FOR_READING: return "file open for reading";
        case PHYSFS_ERR_OPEN_FOR_WRITING: return "file open for writing";
        case PHYSFS_ERR_NOT_A_FILE: return "not a file";
        case PHYSFS_ERR_READ_ONLY: return "read-only filesystem";
        case PHYSFS_ERR_CORRUPT: return "corrupted";
        case PHYSFS_ERR_SYMLINK_LOOP: return "infinite symbolic link loop";
        case PHYSFS_ERR_IO: return "i/o error";
        case PHYSFS_ERR_PERMISSION: return "permission denied";
        case PHYSFS_ERR_NO_SPACE: return "no space available for writing";
        case PHYSFS_ERR_BAD_FILENAME: return "filename is illegal or insecure";
        case PHYSFS_ERR_BUSY: return "tried to modify a file the OS needs";
        case PHYSFS_ERR_DIR_NOT_EMPTY: return "directory isn't empty";
        case PHYSFS_ERR_OS_ERROR: return "OS reported an error";
        case PHYSFS_ERR_DUPLICATE: return "duplicate resource";
        case PHYSFS_ERR_BAD_PASSWORD: return "bad password";
        case PHYSFS_ERR_APP_CALLBACK: return "app callback reported error";
    } /* switch */

    return NULL;  /* don't know this error code. */
} /* PHYSFS_getErrorByCode */


void PHYSFS_setErrorCode(PHYSFS_ErrorCode errcode, const unsigned char dv)
{
    ErrState *err;

    if (!errcode)
        return;

    err = findErrorForCurrentThread(dv);
    if (err == NULL)
    {
        err = (ErrState *) allocator[dv].Malloc(sizeof (ErrState), dv);
        if (err == NULL)
            return;   /* uhh...? */

        memset(err, '\0', sizeof (ErrState));
        err->tid = __PHYSFS_platformGetThreadID();

        if (errorLock[dv] != NULL)
            __PHYSFS_platformGrabMutex(errorLock[dv]);

        err->next = errorStates[dv];
        errorStates[dv] = err;

        if (errorLock[dv] != NULL)
            __PHYSFS_platformReleaseMutex(errorLock[dv]);
    } /* if */

    err->code = errcode;
} /* PHYSFS_setErrorCode */


const char *PHYSFS_getLastError(const unsigned char dv)
{
    const PHYSFS_ErrorCode err = PHYSFS_getLastErrorCode(dv);
    return (err) ? PHYSFS_getErrorByCode(err, dv) : NULL;
} /* PHYSFS_getLastError */


/* MAKE SURE that errorLock[dv] is held before calling this! */
static void freeErrorStates(const unsigned char dv)
{
    ErrState *i;
    ErrState *next;

    for (i = errorStates[dv]; i != NULL; i = next)
    {
        next = i->next;
        allocator[dv].Free(i, dv);
    } /* for */

    errorStates[dv] = NULL;
} /* freeErrorStates */


void PHYSFS_getLinkedVersion(PHYSFS_Version *ver, const unsigned char dv)
{
    if (ver != NULL)
    {
        ver->major = PHYSFS_VER_MAJOR;
        ver->minor = PHYSFS_VER_MINOR;
        ver->patch = PHYSFS_VER_PATCH;
    } /* if */
} /* PHYSFS_getLinkedVersion */


static const char *find_filename_extension(const char *fname, const unsigned char dv)
{
    const char *retval = NULL;
    if (fname != NULL)
    {
        const char *p = strchr(fname, '.');
        retval = p;

        while (p != NULL)
        {
            p = strchr(p + 1, '.');
            if (p != NULL)
                retval = p;
        } /* while */

        if (retval != NULL)
            retval++;  /* skip '.' */
    } /* if */

    return retval;
} /* find_filename_extension */


static DirHandle *tryOpenDir(PHYSFS_Io *io, const PHYSFS_Archiver *funcs,
                             const char *d, int forWriting, int *_claimed, const unsigned char dv)
{
    DirHandle *retval = NULL;
    void *opaque = NULL;

    if (io != NULL)
        BAIL_IF_ERRPASS(!io->seek(io, 0, dv), NULL, dv);

    opaque = funcs->openArchive(io, d, forWriting, _claimed, dv);
    if (opaque != NULL)
    {
        retval = (DirHandle *) allocator[dv].Malloc(sizeof (DirHandle), dv);
        if (retval == NULL)
            funcs->closeArchive(opaque, dv);
        else
        {
            memset(retval, '\0', sizeof (DirHandle));
            retval->mountPoint = NULL;
            retval->funcs = funcs;
            retval->opaque = opaque;
        } /* else */
    } /* if */

    return retval;
} /* tryOpenDir */


static DirHandle *openDirectory(PHYSFS_Io *io, const char *d, int forWriting, const unsigned char dv)
{
    DirHandle *retval = NULL;
    PHYSFS_Archiver **i;
    const char *ext;
    int created_io = 0;
    int claimed = 0;
    PHYSFS_ErrorCode errcode;

    assert((io != NULL) || (d != NULL));

    if (io == NULL)
    {
        /* file doesn't exist, etc? Just fail out. */
        PHYSFS_Stat statbuf;
        BAIL_IF_ERRPASS(!__PHYSFS_platformStat(d, &statbuf, 1, dv), NULL, dv);

        /* DIR gets first shot (unlike the rest, it doesn't deal with files). */
        if (statbuf.filetype == PHYSFS_FILETYPE_DIRECTORY)
        {
            retval = tryOpenDir(io, &__PHYSFS_Archiver_DIR, d, forWriting, &claimed, dv);
            if (retval || claimed)
                return retval;
        } /* if */

        io = __PHYSFS_createNativeIo(d, forWriting ? 'w' : 'r', dv);
        BAIL_IF_ERRPASS(!io, NULL, dv);
        created_io = 1;
    } /* if */

    errcode = currentErrorCode(dv);

    if ((!retval) && (created_io))
        io->destroy(io, dv);

    BAIL_IF(!retval, claimed ? errcode : PHYSFS_ERR_UNSUPPORTED, NULL, dv);
    return retval;
} /* openDirectory */


/*
 * Make a platform-independent path string sane. Doesn't actually check the
 *  file hierarchy, it just cleans up the string.
 *  (dst) must be a buffer at least as big as (src), as this is where the
 *  cleaned up string is deposited.
 * If there are illegal bits in the path (".." entries, etc) then we
 *  return zero and (dst) is undefined. Non-zero if the path was sanitized.
 */
static int sanitizePlatformIndependentPath(const char *src, char *dst, const unsigned char dv)
{
    char *prev;
    char ch;

    while (*src == '/')  /* skip initial '/' chars... */
        src++;

    /* Make sure the entire string isn't "." or ".." */
    if ((strcmp(src, ".") == 0) || (strcmp(src, "..") == 0))
        BAIL(PHYSFS_ERR_BAD_FILENAME, 0, dv);

    prev = dst;
    do
    {
        ch = *(src++);

        if ((ch == ':') || (ch == '\\'))  /* illegal chars in a physfs path. */
            BAIL(PHYSFS_ERR_BAD_FILENAME, 0, dv);

        if (ch == '/')   /* path separator. */
        {
            *dst = '\0';  /* "." and ".." are illegal pathnames. */
            if ((strcmp(prev, ".") == 0) || (strcmp(prev, "..") == 0))
                BAIL(PHYSFS_ERR_BAD_FILENAME, 0, dv);

            while (*src == '/')   /* chop out doubles... */
                src++;

            if (*src == '\0') /* ends with a pathsep? */
                break;  /* we're done, don't add final pathsep to dst. */

            prev = dst + 1;
        } /* if */

        *(dst++) = ch;
    } while (ch != '\0');

    return 1;
} /* sanitizePlatformIndependentPath */


/*
 * Figure out if (fname) is part of (h)'s mountpoint. (fname) must be an
 *  output from sanitizePlatformIndependentPath(), so that it is in a known
 *  state.
 *
 * This only finds legitimate segments of a mountpoint. If the mountpoint is
 *  "/a/b/c" and (fname) is "/a/b/c", "/", or "/a/b/c/d", then the results are
 *  all zero. "/a/b" will succeed, though.
 */
static int partOfMountPoint(DirHandle *h, char *fname, const unsigned char dv)
{
    int rc;
    size_t len, mntpntlen;

    if (h->mountPoint == NULL)
        return 0;
    else if (*fname == '\0')
        return 1;

    len = strlen(fname);
    mntpntlen = strlen(h->mountPoint);
    if (len > mntpntlen)  /* can't be a subset of mountpoint. */
        return 0;

    /* if true, must be not a match or a complete match, but not a subset. */
    if ((len + 1) == mntpntlen)
        return 0;

    rc = strncmp(fname, h->mountPoint, len); /* !!! FIXME: case insensitive? */
    if (rc != 0)
        return 0;  /* not a match. */

    /* make sure /a/b matches /a/b/ and not /a/bc ... */
    return h->mountPoint[len] == '/';
} /* partOfMountPoint */


static DirHandle *createDirHandle(PHYSFS_Io *io, const char *newDir,
                                  const char *mountPoint, int forWriting, const unsigned char dv)
{
    DirHandle *dirHandle = NULL;
    char *tmpmntpnt = NULL;

    assert(newDir != NULL);  /* should have caught this higher up. */

    if (mountPoint != NULL)
    {
        const size_t len = strlen(mountPoint) + 1;
        tmpmntpnt = (char *) __PHYSFS_smallAlloc(len,dv);
        GOTO_IF(!tmpmntpnt, PHYSFS_ERR_OUT_OF_MEMORY, badDirHandle, dv);
        if (!sanitizePlatformIndependentPath(mountPoint, tmpmntpnt, dv))
            goto badDirHandle;
        mountPoint = tmpmntpnt;  /* sanitized version. */
    } /* if */

    dirHandle = openDirectory(io, newDir, forWriting, dv);
    GOTO_IF_ERRPASS(!dirHandle, badDirHandle, dv);

    dirHandle->dirName = (char *) allocator[dv].Malloc(strlen(newDir) + 1, dv);
    GOTO_IF(!dirHandle->dirName, PHYSFS_ERR_OUT_OF_MEMORY, badDirHandle, dv);
    strcpy(dirHandle->dirName, newDir);

    if ((mountPoint != NULL) && (*mountPoint != '\0'))
    {
        dirHandle->mountPoint = (char *)allocator[dv].Malloc(strlen(mountPoint)+2, dv);
        if (!dirHandle->mountPoint)
            GOTO(PHYSFS_ERR_OUT_OF_MEMORY, badDirHandle, dv);
        strcpy(dirHandle->mountPoint, mountPoint);
        strcat(dirHandle->mountPoint, "/");
    } /* if */

    __PHYSFS_smallFree(tmpmntpnt, dv);
    return dirHandle;

badDirHandle:
    if (dirHandle != NULL)
    {
        dirHandle->funcs->closeArchive(dirHandle->opaque, dv);
        allocator[dv].Free(dirHandle->dirName, dv);
        allocator[dv].Free(dirHandle->mountPoint, dv);
        allocator[dv].Free(dirHandle, dv);
    } /* if */

    __PHYSFS_smallFree(tmpmntpnt,dv);
    return NULL;
} /* createDirHandle */


/* MAKE SURE you've got the stateLock[dv] held before calling this! */
static int freeDirHandle(DirHandle *dh, FileHandle *openList, const unsigned char dv)
{
    FileHandle *i;

    if (dh == NULL)
        return 1;

    for (i = openList; i != NULL; i = i->next)
        BAIL_IF(i->dirHandle == dh, PHYSFS_ERR_FILES_STILL_OPEN, 0, dv);

    dh->funcs->closeArchive(dh->opaque, dv);
    allocator[dv].Free(dh->dirName, dv);
    allocator[dv].Free(dh->mountPoint, dv);
    allocator[dv].Free(dh, dv);
    return 1;
} /* freeDirHandle */


static char *calculateBaseDir(const char *argv0, const unsigned char dv)
{
    const char dirsep = __PHYSFS_platformDirSeparator;
    char *retval = NULL;
    char *ptr = NULL;

    /* Give the platform layer first shot at this. */
    retval = __PHYSFS_platformCalcBaseDir(argv0, dv);
    if (retval != NULL)
        return retval;

    /* We need argv0 to go on. */
    BAIL_IF(argv0 == NULL, PHYSFS_ERR_ARGV0_IS_NULL, NULL, dv);

    ptr = strrchr(argv0, dirsep);
    if (ptr != NULL)
    {
        const size_t size = ((size_t) (ptr - argv0)) + 1;
        retval = (char *) allocator[dv].Malloc(size + 1, dv);
        BAIL_IF(!retval, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
        memcpy(retval, argv0, size);
        retval[size] = '\0';
        return retval;
    } /* if */

    /* argv0 wasn't helpful. */
    BAIL(PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);
} /* calculateBaseDir */


static int initializeMutexes(const unsigned char dv)
{
    errorLock[dv] = __PHYSFS_platformCreateMutex(dv);
    if (errorLock[dv] == NULL)
        goto initializeMutexes_failed;

    stateLock[dv] = __PHYSFS_platformCreateMutex(dv);
    if (stateLock[dv] == NULL)
        goto initializeMutexes_failed;

    return 1;  /* success. */

initializeMutexes_failed:
    if (errorLock[dv] != NULL)
        __PHYSFS_platformDestroyMutex(errorLock[dv],dv);

    if (stateLock[dv] != NULL)
        __PHYSFS_platformDestroyMutex(stateLock[dv],dv);

    errorLock[dv] = stateLock[dv] = NULL;
    return 0;  /* failed. */
} /* initializeMutexes */


static void setDefaultAllocator(const unsigned char dv);
static int doDeinit(const unsigned char dv);

int PHYSFS_init(const char *argv0, const unsigned char dv)
{
    // check if one time multi initialization needs to be done
    if (multi_initialized == 0) {
	    for (unsigned char d=0; d< NUM_DRIVES;d++) {
	    initialized[d] = 0;
	    errorStates[d] = NULL;
	    searchPath[d] = NULL;
	    writeDir[d] = NULL;
	    openWriteList[d] = NULL;
	    openReadList[d] = NULL;
	    baseDir[d] = NULL;
	    userDir[d] = NULL;
	    prefDir[d] = NULL;
	    allowSymLinks[d] = 0;
	    numArchivers[d] = 0;

	    /* mutexes ... */
	    errorLock[d] = NULL;     /* protects error message list.        */
	    stateLock[d] = NULL;     /* protects other PhysFS static state. */

	    /* allocator ... */
	    externalAllocator[d] = 0;
	    }
	    multi_initialized = 1;
    }


    BAIL_IF(initialized[dv], PHYSFS_ERR_IS_INITIALIZED, 0, dv);

    if (!externalAllocator[dv])
        setDefaultAllocator(dv);

    if ((allocator[dv].Init != NULL) && (!allocator[dv].Init(dv))) return 0;

    if (!initializeMutexes(dv)) goto initFailed;

    baseDir[dv] = calculateBaseDir(argv0, dv);
    if (!baseDir[dv]) goto initFailed;

    userDir[dv] = __PHYSFS_platformCalcUserDir(dv);
    if (!userDir[dv]) goto initFailed;

    /* Platform layer is required to append a dirsep. */
    assert(baseDir[dv][strlen(baseDir[dv]) - 1] == __PHYSFS_platformDirSeparator);
    assert(userDir[dv][strlen(userDir[dv]) - 1] == __PHYSFS_platformDirSeparator);

    initialized[dv] = 1;

    /* This makes sure that the error subsystem is initialized[dv]. */
    PHYSFS_setErrorCode(PHYSFS_getLastErrorCode(dv),dv);

    return 1;

initFailed:
    doDeinit(dv);
    return 0;
} /* PHYSFS_init */


/* MAKE SURE you hold stateLock[dv] before calling this! */
static int closeFileHandleList(FileHandle **list, const unsigned char dv)
{
    FileHandle *i;
    FileHandle *next = NULL;

    for (i = *list; i != NULL; i = next)
    {
        PHYSFS_Io *io = i->io;
        next = i->next;

        if (io->flush && !io->flush(io,dv))
        {
            *list = i;
            return 0;
        } /* if */

        io->destroy(io, dv);
        allocator[dv].Free(i, dv);
    } /* for */

    *list = NULL;
    return 1;
} /* closeFileHandleList */


/* MAKE SURE you hold the stateLock[dv] before calling this! */
static void freeSearchPath(const unsigned char dv)
{
    DirHandle *i;
    DirHandle *next = NULL;

    closeFileHandleList(&openReadList[dv], dv);

    if (searchPath[dv] != NULL)
    {
        for (i = searchPath[dv]; i != NULL; i = next)
        {
            next = i->next;
            freeDirHandle(i, openReadList[dv], dv);
        } /* for */
        searchPath[dv] = NULL;
    } /* if */
} /* freeSearchPath */


static int doDeinit(const unsigned char dv)
{
    closeFileHandleList(&openWriteList[dv], dv);
    BAIL_IF(!PHYSFS_setWriteDir(NULL, dv), PHYSFS_ERR_FILES_STILL_OPEN, 0, dv);

    freeSearchPath(dv);
    freeErrorStates(dv);

    if (baseDir[dv] != NULL)
    {
        allocator[dv].Free(baseDir[dv], dv);
        baseDir[dv] = NULL;
    } /* if */

    if (userDir[dv] != NULL)
    {
        allocator[dv].Free(userDir[dv], dv);
        userDir[dv] = NULL;
    } /* if */

    if (prefDir[dv] != NULL)
    {
        allocator[dv].Free(prefDir[dv], dv);
        prefDir[dv] = NULL;
    } /* if */


    allowSymLinks[dv] = 0;
    initialized[dv] = 0;

    if (errorLock[dv]) __PHYSFS_platformDestroyMutex(errorLock[dv], dv);
    if (stateLock[dv]) __PHYSFS_platformDestroyMutex(stateLock[dv], dv);

    if (allocator[dv].Deinit != NULL)
        allocator[dv].Deinit(dv);

    errorLock[dv] = stateLock[dv] = NULL;

    return 1;
} /* doDeinit */


int PHYSFS_deinit(const unsigned char dv)
{
    BAIL_IF(!initialized[dv], PHYSFS_ERR_NOT_INITIALIZED, 0, dv);
    return doDeinit(dv);
} /* PHYSFS_deinit */


int PHYSFS_isInit(const unsigned char dv)
{
    return initialized[dv];
} /* PHYSFS_isInit */


char *__PHYSFS_strdup(const char *str, const unsigned char dv)
{
    char *retval = (char *) allocator[dv].Malloc(strlen(str) + 1, dv);
    if (retval)
        strcpy(retval, str);
    return retval;
} /* __PHYSFS_strdup */


PHYSFS_uint32 __PHYSFS_hashString(const char *str, size_t len)
{
    PHYSFS_uint32 hash = 5381;
    while (len--)
        hash = ((hash << 5) + hash) ^ *(str++);
    return hash;
} /* __PHYSFS_hashString */


void PHYSFS_freeList(void *list, const unsigned char dv)
{
    void **i;
    if (list != NULL)
    {
        for (i = (void **) list; *i != NULL; i++)
            allocator[dv].Free(*i, dv);

        allocator[dv].Free(list, dv);
    } /* if */
} /* PHYSFS_freeList */


const char *PHYSFS_getDirSeparator(const unsigned char dv)
{
    static char retval[2] = { __PHYSFS_platformDirSeparator, '\0' };
    return retval;
} /* PHYSFS_getDirSeparator */


const char *PHYSFS_getPrefDir(const char *org, const char *app, const unsigned char dv)
{
    const char dirsep = __PHYSFS_platformDirSeparator;
    PHYSFS_Stat statbuf;
    char *ptr = NULL;
    char *endstr = NULL;

    BAIL_IF(!initialized[dv], PHYSFS_ERR_NOT_INITIALIZED, 0, dv);
    BAIL_IF(!org, PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);
    BAIL_IF(*org == '\0', PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);
    BAIL_IF(!app, PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);
    BAIL_IF(*app == '\0', PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);

    allocator[dv].Free(prefDir[dv], dv);
    prefDir[dv] = __PHYSFS_platformCalcPrefDir(org, app, dv);
    BAIL_IF_ERRPASS(!prefDir[dv], NULL, dv);

    assert(strlen(prefDir[dv]) > 0);
    endstr = prefDir[dv] + (strlen(prefDir[dv]) - 1);
    assert(*endstr == dirsep);
    *endstr = '\0';  /* mask out the final dirsep for now. */

    if (!__PHYSFS_platformStat(prefDir[dv], &statbuf, 1, dv))
    {
        for (ptr = strchr(prefDir[dv], dirsep); ptr; ptr = strchr(ptr+1, dirsep))
        {
            *ptr = '\0';
            __PHYSFS_platformMkDir(prefDir[dv],dv);
            *ptr = dirsep;
        } /* for */

        if (!__PHYSFS_platformMkDir(prefDir[dv],dv))
        {
            allocator[dv].Free(prefDir[dv], dv);
            prefDir[dv] = NULL;
        } /* if */
    } /* if */

    *endstr = dirsep;  /* readd the final dirsep. */

    return prefDir[dv];
} /* PHYSFS_getPrefDir */


const char *PHYSFS_getBaseDir(const unsigned char dv)
{
    return baseDir[dv];   /* this is calculated in PHYSFS_init()... */
} /* PHYSFS_getBaseDir */


const char *__PHYSFS_getUserDir(const unsigned char dv)  /* not deprecated internal version. */
{
    return userDir[dv];   /* this is calculated in PHYSFS_init()... */
} /* __PHYSFS_getUserDir */


const char *PHYSFS_getUserDir(const unsigned char dv)
{
    return __PHYSFS_getUserDir(dv);
} /* PHYSFS_getUserDir */


const char *PHYSFS_getWriteDir(const unsigned char dv)
{
    const char *retval = NULL;

    __PHYSFS_platformGrabMutex(stateLock[dv]);
    if (writeDir[dv] != NULL)
        retval = writeDir[dv]->dirName;
    __PHYSFS_platformReleaseMutex(stateLock[dv]);

    return retval;
} /* PHYSFS_getWriteDir */


int PHYSFS_setWriteDir(const char *newDir, const unsigned char dv)
{
    int retval = 1;

    __PHYSFS_platformGrabMutex(stateLock[dv]);

    if (writeDir[dv] != NULL)
    {
        BAIL_IF_MUTEX_ERRPASS(!freeDirHandle(writeDir[dv], openWriteList[dv], dv),
                            stateLock[dv], 0, dv);
        writeDir[dv] = NULL;
    } /* if */

    if (newDir != NULL)
    {
        writeDir[dv] = createDirHandle(NULL, newDir, NULL, 1, dv);
        retval = (writeDir[dv] != NULL);
    } /* if */

    __PHYSFS_platformReleaseMutex(stateLock[dv]);

    return retval;
} /* PHYSFS_setWriteDir */


static int doMount(PHYSFS_Io *io, const char *fname,
                   const char *mountPoint, int appendToPath, const unsigned char dv)
{
    DirHandle *dh;
    DirHandle *prev = NULL;
    DirHandle *i;

    BAIL_IF(!fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);

    if (mountPoint == NULL)
        mountPoint = "/";

    __PHYSFS_platformGrabMutex(stateLock[dv]);

    for (i = searchPath[dv]; i != NULL; i = i->next)
    {
        /* already in search path? */
        if ((i->dirName != NULL) && (strcmp(fname, i->dirName) == 0))
            BAIL_MUTEX_ERRPASS(stateLock[dv], 1);
        prev = i;
    } /* for */

    dh = createDirHandle(io, fname, mountPoint, 0, dv);
    BAIL_IF_MUTEX_ERRPASS(!dh, stateLock[dv], 0, dv);

    if (appendToPath)
    {
        if (prev == NULL)
            searchPath[dv] = dh;
        else
            prev->next = dh;
    } /* if */
    else
    {
        dh->next = searchPath[dv];
        searchPath[dv] = dh;
    } /* else */

    __PHYSFS_platformReleaseMutex(stateLock[dv]);
    return 1;
} /* doMount */


int PHYSFS_mountIo(PHYSFS_Io *io, const char *fname,
                   const char *mountPoint, int appendToPath, const unsigned char dv)
{
    BAIL_IF(!io, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    BAIL_IF(!fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    BAIL_IF(io->version != 0, PHYSFS_ERR_UNSUPPORTED, 0, dv);
    return doMount(io, fname, mountPoint, appendToPath, dv);
} /* PHYSFS_mountIo */


int PHYSFS_mountHandle(PHYSFS_File *file, const char *fname,
                       const char *mountPoint, int appendToPath, const unsigned char dv)
{
    int retval = 0;
    PHYSFS_Io *io = NULL;

    BAIL_IF(!file, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    BAIL_IF(!fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);

    io = __PHYSFS_createHandleIo(file, dv);
    BAIL_IF_ERRPASS(!io, 0, dv);
    retval = doMount(io, fname, mountPoint, appendToPath, dv);
    if (!retval)
    {
        /* docs say not to destruct in case of failure, so cheat. */
        io->opaque = NULL;
        io->destroy(io, dv);
    } /* if */

    return retval;
} /* PHYSFS_mountHandle */


int PHYSFS_mount(const char *newDir, const char *mountPoint, int appendToPath, const unsigned char dv)
{
    BAIL_IF(!newDir, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    return doMount(NULL, newDir, mountPoint, appendToPath, dv);
} /* PHYSFS_mount */


int PHYSFS_addToSearchPath(const char *newDir, int appendToPath, const unsigned char dv)
{
    return PHYSFS_mount(newDir, NULL, appendToPath, dv);
} /* PHYSFS_addToSearchPath */


int PHYSFS_removeFromSearchPath(const char *oldDir, const unsigned char dv)
{
    return PHYSFS_unmount(oldDir, dv);
} /* PHYSFS_removeFromSearchPath */


int PHYSFS_unmount(const char *oldDir, const unsigned char dv)
{
    DirHandle *i;
    DirHandle *prev = NULL;
    DirHandle *next = NULL;

    BAIL_IF(oldDir == NULL, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);

    __PHYSFS_platformGrabMutex(stateLock[dv]);
    for (i = searchPath[dv]; i != NULL; i = i->next)
    {
        if (strcmp(i->dirName, oldDir) == 0)
        {
            next = i->next;
            BAIL_IF_MUTEX_ERRPASS(!freeDirHandle(i, openReadList[dv], dv),
                                stateLock[dv], 0, dv);

            if (prev == NULL)
                searchPath[dv] = next;
            else
                prev->next = next;

            BAIL_MUTEX_ERRPASS(stateLock[dv], 1);
        } /* if */
        prev = i;
    } /* for */

    BAIL_MUTEX(PHYSFS_ERR_NOT_MOUNTED, stateLock[dv], 0, dv);
} /* PHYSFS_unmount */


char **PHYSFS_getSearchPath(const unsigned char dv)
{
    return doEnumStringList(PHYSFS_getSearchPathCallback, dv);
} /* PHYSFS_getSearchPath */


const char *PHYSFS_getMountPoint(const char *dir, const unsigned char dv)
{
    DirHandle *i;
    __PHYSFS_platformGrabMutex(stateLock[dv]);
    for (i = searchPath[dv]; i != NULL; i = i->next)
    {
        if (strcmp(i->dirName, dir) == 0)
        {
            const char *retval = ((i->mountPoint) ? i->mountPoint : "/");
            __PHYSFS_platformReleaseMutex(stateLock[dv]);
            return retval;
        } /* if */
    } /* for */
    __PHYSFS_platformReleaseMutex(stateLock[dv]);

    BAIL(PHYSFS_ERR_NOT_MOUNTED, NULL, dv);
} /* PHYSFS_getMountPoint */


void PHYSFS_getSearchPathCallback(PHYSFS_StringCallback callback, void *data, const unsigned char dv)
{
    DirHandle *i;

    __PHYSFS_platformGrabMutex(stateLock[dv]);

    for (i = searchPath[dv]; i != NULL; i = i->next)
        callback(data, i->dirName, dv);

    __PHYSFS_platformReleaseMutex(stateLock[dv]);
} /* PHYSFS_getSearchPathCallback */


typedef struct setSaneCfgEnumData
{
    const char *archiveExt;
    size_t archiveExtLen;
    int archivesFirst;
    PHYSFS_ErrorCode errcode;
} setSaneCfgEnumData;

static PHYSFS_EnumerateCallbackResult setSaneCfgEnumCallback(void *_data,
                                                const char *dir, const char *f, const unsigned char dv)
{
    setSaneCfgEnumData *data = (setSaneCfgEnumData *) _data;
    const size_t extlen = data->archiveExtLen;
    const size_t l = strlen(f);
    const char *ext;

    if ((l > extlen) && (f[l - extlen - 1] == '.'))
    {
        ext = f + (l - extlen);
        if (PHYSFS_utf8stricmp(ext, data->archiveExt) == 0)
        {
            const char dirsep = __PHYSFS_platformDirSeparator;
            const char *d = PHYSFS_getRealDir(f,dv);
            const size_t allocsize = strlen(d) + l + 2;
            char *str = (char *) __PHYSFS_smallAlloc(allocsize,dv);
            if (str == NULL)
                data->errcode = PHYSFS_ERR_OUT_OF_MEMORY;
            else
            {
                snprintf(str, allocsize, "%s%c%s", d, dirsep, f);
                if (!PHYSFS_mount(str, NULL, data->archivesFirst == 0, dv))
                    data->errcode = currentErrorCode(dv);
                __PHYSFS_smallFree(str,dv);
            } /* else */
        } /* if */
    } /* if */

    /* !!! FIXME: if we want to abort on errors... */
    /*return (data->errcode != PHYSFS_ERR_OK) ? PHYSFS_ENUM_ERROR : PHYSFS_ENUM_OK;*/

    return PHYSFS_ENUM_OK;  /* keep going */
} /* setSaneCfgEnumCallback */


int PHYSFS_setSaneConfig(const char *organization, const char *appName,
                         const char *archiveExt, int includeCdRoms,
                         int archivesFirst, const unsigned char dv)
{
    const char *basedir;
    const char *prefdir;

    BAIL_IF(!initialized[dv], PHYSFS_ERR_NOT_INITIALIZED, 0, dv);

    prefdir = PHYSFS_getPrefDir(organization, appName, dv);
    BAIL_IF_ERRPASS(!prefdir, 0, dv);

    basedir = PHYSFS_getBaseDir(dv);
    BAIL_IF_ERRPASS(!basedir, 0, dv);

    BAIL_IF(!PHYSFS_setWriteDir(prefdir, dv), PHYSFS_ERR_NO_WRITE_DIR, 0, dv);

    /* !!! FIXME: these can fail and we should report that... */

    /* Put write dir first in search path... */
    PHYSFS_mount(prefdir, NULL, 0, dv);

    /* Put base path on search path... */
    PHYSFS_mount(basedir, NULL, 1, dv);

    /* Root out archives, and add them to search path... */
    if (archiveExt != NULL)
    {
        setSaneCfgEnumData data;
        memset(&data, '\0', sizeof (data));
        data.archiveExt = archiveExt;
        data.archiveExtLen = strlen(archiveExt);
        data.archivesFirst = archivesFirst;
        data.errcode = PHYSFS_ERR_OK;
        if (!PHYSFS_enumerate("/", setSaneCfgEnumCallback, &data, dv))
        {
            /* !!! FIXME: use this if we're reporting errors.
            PHYSFS_ErrorCode errcode = currentErrorCode(dv);
            if (errcode == PHYSFS_ERR_APP_CALLBACK)
                errcode = data->errcode; */
        } /* if */
    } /* if */

    return 1;
} /* PHYSFS_setSaneConfig */


void PHYSFS_permitSymbolicLinks(int allow, const unsigned char dv)
{
    allowSymLinks[dv] = allow;
} /* PHYSFS_permitSymbolicLinks */


int PHYSFS_symbolicLinksPermitted(const unsigned char dv)
{
    return allowSymLinks[dv];
} /* PHYSFS_symbolicLinksPermitted */


/*
 * Verify that (fname) (in platform-independent notation), in relation
 *  to (h) is secure. That means that each element of fname is checked
 *  for symlinks (if they aren't permitted). This also allows for quick
 *  rejection of files that exist outside an archive's mountpoint.
 *
 * With some exceptions (like PHYSFS_mkdir(), which builds multiple subdirs
 *  at a time), you should always pass zero for "allowMissing" for efficiency.
 *
 * (fname) must point to an output from sanitizePlatformIndependentPath(),
 *  since it will make sure that path names are in the right format for
 *  passing certain checks. It will also do checks for "insecure" pathnames
 *  like ".." which should be done once instead of once per archive. This also
 *  gives us license to treat (fname) as scratch space in this function.
 *
 * Returns non-zero if string is safe, zero if there's a security issue.
 *  PHYSFS_getLastError() will specify what was wrong. (*fname) will be
 *  updated to point past any mount point elements so it is prepared to
 *  be used with the archiver directly.
 */
static int verifyPath(DirHandle *h, char **_fname, int allowMissing, const unsigned char dv)
{
    char *fname = *_fname;
    int retval = 1;
    char *start;
    char *end;

    if (*fname == '\0')  /* quick rejection. */
        return 1;

    /* !!! FIXME: This codeblock sucks. */
    if (h->mountPoint != NULL)  /* NULL mountpoint means "/". */
    {
        size_t mntpntlen = strlen(h->mountPoint);
        size_t len = strlen(fname);
        assert(mntpntlen > 1); /* root mount points should be NULL. */
        /* not under the mountpoint, so skip this archive. */
        BAIL_IF(len < mntpntlen-1, PHYSFS_ERR_NOT_FOUND, 0, dv);
        /* !!! FIXME: Case insensitive? */
        retval = strncmp(h->mountPoint, fname, mntpntlen-1);
        BAIL_IF(retval != 0, PHYSFS_ERR_NOT_FOUND, 0, dv);
        if (len > mntpntlen-1)  /* corner case... */
            BAIL_IF(fname[mntpntlen-1]!='/', PHYSFS_ERR_NOT_FOUND, 0, dv);
        fname += mntpntlen-1;  /* move to start of actual archive path. */
        if (*fname == '/')
            fname++;
        *_fname = fname;  /* skip mountpoint for later use. */
        retval = 1;  /* may be reset, below. */
    } /* if */

    start = fname;
    if (!allowSymLinks[dv])
    {
        while (1)
        {
            PHYSFS_Stat statbuf;
            int rc = 0;
            end = strchr(start, '/');

            if (end != NULL) *end = '\0';
            rc = h->funcs->stat(h->opaque, fname, &statbuf, dv);
            if (rc)
                rc = (statbuf.filetype == PHYSFS_FILETYPE_SYMLINK);
            else if (currentErrorCode(dv) == PHYSFS_ERR_NOT_FOUND)
                retval = 0;

            if (end != NULL) *end = '/';

            /* insecure path (has a disallowed symlink in it)? */
            BAIL_IF(rc, PHYSFS_ERR_SYMLINK_FORBIDDEN, 0, dv);

            /* break out early if path element is missing. */
            if (!retval)
            {
                /*
                 * We need to clear it if it's the last element of the path,
                 *  since this might be a non-existant file we're opening
                 *  for writing...
                 */
                if ((end == NULL) || (allowMissing))
                    retval = 1;
                break;
            } /* if */

            if (end == NULL)
                break;

            start = end + 1;
        } /* while */
    } /* if */

    return retval;
} /* verifyPath */


static int doMkdir(const char *_dname, char *dname, const unsigned char dv)
{
    DirHandle *h;
    char *start;
    char *end;
    int retval = 0;
    int exists = 1;  /* force existance check on first path element. */

    BAIL_IF_ERRPASS(!sanitizePlatformIndependentPath(_dname, dname, dv), 0, dv);

    __PHYSFS_platformGrabMutex(stateLock[dv]);
    BAIL_IF_MUTEX(!writeDir[dv], PHYSFS_ERR_NO_WRITE_DIR, stateLock[dv], 0, dv);
    h = writeDir[dv];
    BAIL_IF_MUTEX_ERRPASS(!verifyPath(h, &dname, 1, dv), stateLock[dv], 0, dv);

    start = dname;
    while (1)
    {
        end = strchr(start, '/');
        if (end != NULL)
            *end = '\0';

        /* only check for existance if all parent dirs existed, too... */
        if (exists)
        {
            PHYSFS_Stat statbuf;
            const int rc = h->funcs->stat(h->opaque, dname, &statbuf, dv);
            if ((!rc) && (currentErrorCode(dv) == PHYSFS_ERR_NOT_FOUND))
                exists = 0;
            retval = ((rc) && (statbuf.filetype == PHYSFS_FILETYPE_DIRECTORY));
        } /* if */

        if (!exists)
            retval = h->funcs->mkdir(h->opaque, dname, dv);

        if (!retval)
            break;

        if (end == NULL)
            break;

        *end = '/';
        start = end + 1;
    } /* while */

    __PHYSFS_platformReleaseMutex(stateLock[dv]);
    return retval;
} /* doMkdir */


int PHYSFS_mkdir(const char *_dname, const unsigned char dv)
{
    int retval = 0;
    char *dname;
    size_t len;

    BAIL_IF(!_dname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    len = strlen(_dname) + 1;
    dname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!dname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);
    retval = doMkdir(_dname, dname, dv);
    __PHYSFS_smallFree(dname,dv);
    return retval;
} /* PHYSFS_mkdir */


static int doDelete(const char *_fname, char *fname, const unsigned char dv)
{
    int retval;
    DirHandle *h;
    BAIL_IF_ERRPASS(!sanitizePlatformIndependentPath(_fname, fname, dv), 0, dv);

    __PHYSFS_platformGrabMutex(stateLock[dv]);

    BAIL_IF_MUTEX(!writeDir[dv], PHYSFS_ERR_NO_WRITE_DIR, stateLock[dv], 0, dv);
    h = writeDir[dv];
    BAIL_IF_MUTEX_ERRPASS(!verifyPath(h, &fname, 0, dv), stateLock[dv], 0, dv);
    retval = h->funcs->remove(h->opaque, fname, dv);

    __PHYSFS_platformReleaseMutex(stateLock[dv]);
    return retval;
} /* doDelete */


int PHYSFS_delete(const char *_fname, const unsigned char dv)
{
    int retval;
    char *fname;
    size_t len;

    BAIL_IF(!_fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    len = strlen(_fname) + 1;
    fname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);
    retval = doDelete(_fname, fname, dv);
    __PHYSFS_smallFree(fname, dv);
    return retval;
} /* PHYSFS_delete */


static DirHandle *getRealDirHandle(const char *_fname, const unsigned char dv)
{
    DirHandle *retval = NULL;
    char *fname = NULL;
    size_t len;

    BAIL_IF(!_fname, PHYSFS_ERR_INVALID_ARGUMENT, NULL, dv);
    len = strlen(_fname) + 1;
    fname = __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    if (sanitizePlatformIndependentPath(_fname, fname, dv))
    {
        DirHandle *i;
        __PHYSFS_platformGrabMutex(stateLock[dv]);
        for (i = searchPath[dv]; i != NULL; i = i->next)
        {
            char *arcfname = fname;
            if (partOfMountPoint(i, arcfname, dv))
            {
                retval = i;
                break;
            } /* if */
            else if (verifyPath(i, &arcfname, 0, dv))
            {
                PHYSFS_Stat statbuf;
                if (i->funcs->stat(i->opaque, arcfname, &statbuf, dv))
                {
                    retval = i;
                    break;
                } /* if */
            } /* if */
        } /* for */
        __PHYSFS_platformReleaseMutex(stateLock[dv]);
    } /* if */

    __PHYSFS_smallFree(fname, dv);
    return retval;
} /* getRealDirHandle */

const char *PHYSFS_getRealDir(const char *fname, const unsigned char dv)
{
    DirHandle *dh = getRealDirHandle(fname, dv);
    return dh ? dh->dirName : NULL;
} /* PHYSFS_getRealDir */


static int locateInStringList(const char *str,
                              char **list,
                              PHYSFS_uint32 *pos)
{
    PHYSFS_uint32 len = *pos;
    PHYSFS_uint32 half_len;
    PHYSFS_uint32 lo = 0;
    PHYSFS_uint32 middle;
    int cmp;

    while (len > 0)
    {
        half_len = len >> 1;
        middle = lo + half_len;
        cmp = strcmp(list[middle], str);

        if (cmp == 0)  /* it's in the list already. */
            return 1;
        else if (cmp > 0)
            len = half_len;
        else
        {
            lo = middle + 1;
            len -= half_len + 1;
        } /* else */
    } /* while */

    *pos = lo;
    return 0;
} /* locateInStringList */


static PHYSFS_EnumerateCallbackResult enumFilesCallback(void *data,
                                        const char *origdir, const char *str, const unsigned char dv)
{
    PHYSFS_uint32 pos;
    void *ptr;
    char *newstr;
    EnumStringListCallbackData *pecd = (EnumStringListCallbackData *) data;

    /*
     * See if file is in the list already, and if not, insert it in there
     *  alphabetically...
     */
    pos = pecd->size;
    if (locateInStringList(str, pecd->list, &pos))
        return PHYSFS_ENUM_OK;  /* already in the list, but keep going. */

    ptr = allocator[dv].Realloc(pecd->list, (pecd->size + 2) * sizeof (char *), dv);
    newstr = (char *) allocator[dv].Malloc(strlen(str) + 1, dv);
    if (ptr != NULL)
        pecd->list = (char **) ptr;

    if ((ptr == NULL) || (newstr == NULL))
    {
        if (newstr)
            allocator[dv].Free(newstr, dv);

        pecd->errcode = PHYSFS_ERR_OUT_OF_MEMORY;
        return PHYSFS_ENUM_ERROR;  /* better luck next time. */
    } /* if */

    strcpy(newstr, str);

    if (pos != pecd->size)
    {
        memmove(&pecd->list[pos+1], &pecd->list[pos],
                 sizeof (char *) * ((pecd->size) - pos));
    } /* if */

    pecd->list[pos] = newstr;
    pecd->size++;

    return PHYSFS_ENUM_OK;
} /* enumFilesCallback */


char **PHYSFS_enumerateFiles(const char *path, const unsigned char dv)
{
    EnumStringListCallbackData ecd;
    memset(&ecd, '\0', sizeof (ecd));
    ecd.list = (char **) allocator[dv].Malloc(sizeof (char *), dv);
    BAIL_IF(!ecd.list, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    if (!PHYSFS_enumerate(path, enumFilesCallback, &ecd, dv))
    {
        const PHYSFS_ErrorCode errcode = currentErrorCode(dv);
        PHYSFS_uint32 i;
        for (i = 0; i < ecd.size; i++)
            allocator[dv].Free(ecd.list[i], dv);
        allocator[dv].Free(ecd.list, dv);
        BAIL_IF(errcode == PHYSFS_ERR_APP_CALLBACK, ecd.errcode, NULL, dv);
        return NULL;
    } /* if */

    ecd.list[ecd.size] = NULL;
    return ecd.list;
} /* PHYSFS_enumerateFiles */


/*
 * Broke out to seperate function so we can use stack allocation gratuitously.
 */
static PHYSFS_EnumerateCallbackResult enumerateFromMountPoint(DirHandle *i,
                                    const char *arcfname,
                                    PHYSFS_EnumerateCallback callback,
                                    const char *_fname, void *data, const unsigned char dv)
{
    PHYSFS_EnumerateCallbackResult retval;
    const size_t len = strlen(arcfname);
    char *ptr = NULL;
    char *end = NULL;
    const size_t slen = strlen(i->mountPoint) + 1;
    char *mountPoint = (char *) __PHYSFS_smallAlloc(slen,dv);

    BAIL_IF(!mountPoint, PHYSFS_ERR_OUT_OF_MEMORY, PHYSFS_ENUM_ERROR, dv);

    strcpy(mountPoint, i->mountPoint);
    ptr = mountPoint + ((len) ? len + 1 : 0);
    end = strchr(ptr, '/');
    assert(end);  /* should always find a terminating '/'. */
    *end = '\0';
    retval = callback(data, _fname, ptr, dv);
    __PHYSFS_smallFree(mountPoint, dv);

    BAIL_IF(retval == PHYSFS_ENUM_ERROR, PHYSFS_ERR_APP_CALLBACK, retval, dv);
    return retval;
} /* enumerateFromMountPoint */


typedef struct SymlinkFilterData
{
    PHYSFS_EnumerateCallback callback;
    void *callbackData;
    DirHandle *dirhandle;
    const char *arcfname;
    PHYSFS_ErrorCode errcode;
} SymlinkFilterData;

static PHYSFS_EnumerateCallbackResult enumCallbackFilterSymLinks(void *_data,
                                    const char *origdir, const char *fname, const unsigned char dv)
{
    SymlinkFilterData *data = (SymlinkFilterData *) _data;
    const DirHandle *dh = data->dirhandle;
    const char *arcfname = data->arcfname;
    PHYSFS_Stat statbuf;
    const char *trimmedDir = (*arcfname == '/') ? (arcfname + 1) : arcfname;
    const size_t slen = strlen(trimmedDir) + strlen(fname) + 2;
    char *path = (char *) __PHYSFS_smallAlloc(slen,dv);
    PHYSFS_EnumerateCallbackResult retval = PHYSFS_ENUM_OK;

    if (path == NULL)
    {
        data->errcode = PHYSFS_ERR_OUT_OF_MEMORY;
        return PHYSFS_ENUM_ERROR;
    } /* if */

    snprintf(path, slen, "%s%s%s", trimmedDir, *trimmedDir ? "/" : "", fname);

    if (!dh->funcs->stat(dh->opaque, path, &statbuf, dv))
    {
        data->errcode = currentErrorCode(dv);
        retval = PHYSFS_ENUM_ERROR;
    } /* if */
    else
    {
        /* Pass it on to the application if it's not a symlink. */
        if (statbuf.filetype != PHYSFS_FILETYPE_SYMLINK)
        {
            retval = data->callback(data->callbackData, origdir, fname, dv);
            if (retval == PHYSFS_ENUM_ERROR)
                data->errcode = PHYSFS_ERR_APP_CALLBACK;
        } /* if */
    } /* else */

    __PHYSFS_smallFree(path, dv);

    return retval;
} /* enumCallbackFilterSymLinks */


int PHYSFS_enumerate(const char *_fn, PHYSFS_EnumerateCallback cb, void *data, const unsigned char dv)
{
    PHYSFS_EnumerateCallbackResult retval = PHYSFS_ENUM_OK;
    size_t len;
    char *fname;

    BAIL_IF(!_fn, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    BAIL_IF(!cb, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);

    len = strlen(_fn) + 1;
    fname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);

    if (!sanitizePlatformIndependentPath(_fn, fname, dv))
        retval = PHYSFS_ENUM_STOP;
    else
    {
        DirHandle *i;
        SymlinkFilterData filterdata;

        __PHYSFS_platformGrabMutex(stateLock[dv]);

        if (!allowSymLinks[dv])
        {
            memset(&filterdata, '\0', sizeof (filterdata));
            filterdata.callback = cb;
            filterdata.callbackData = data;
        } /* if */

        for (i = searchPath[dv]; (retval == PHYSFS_ENUM_OK) && i; i = i->next)
        {
            char *arcfname = fname;

            if (partOfMountPoint(i, arcfname, dv))
                retval = enumerateFromMountPoint(i, arcfname, cb, _fn, data, dv);

            else if (verifyPath(i, &arcfname, 0, dv))
            {
                PHYSFS_Stat statbuf;
                if (!i->funcs->stat(i->opaque, arcfname, &statbuf, dv))
                {
                    if (currentErrorCode(dv) == PHYSFS_ERR_NOT_FOUND)
                        continue;  /* no such dir in this archive, skip it. */
                } /* if */

                if (statbuf.filetype != PHYSFS_FILETYPE_DIRECTORY)
                    continue;  /* not a directory in this archive, skip it. */

                else if ((!allowSymLinks[dv]) && (i->funcs->info.supportsSymlinks))
                {
                    filterdata.dirhandle = i;
                    filterdata.arcfname = arcfname;
                    filterdata.errcode = PHYSFS_ERR_OK;
                    retval = i->funcs->enumerate(i->opaque, arcfname,
                                                 enumCallbackFilterSymLinks,
                                                 _fn, &filterdata, dv);
                    if (retval == PHYSFS_ENUM_ERROR)
                    {
                        if (currentErrorCode(dv) == PHYSFS_ERR_APP_CALLBACK)
                            PHYSFS_setErrorCode(filterdata.errcode, dv);
                    } /* if */
                } /* else if */
                else
                {
                    retval = i->funcs->enumerate(i->opaque, arcfname,
                                                 cb, _fn, data, dv);
                } /* else */
            } /* else if */
        } /* for */

        __PHYSFS_platformReleaseMutex(stateLock[dv]);
    } /* if */

    __PHYSFS_smallFree(fname, dv);

    return (retval == PHYSFS_ENUM_ERROR) ? 0 : 1;
} /* PHYSFS_enumerate */


typedef struct
{
    PHYSFS_EnumFilesCallback callback;
    void *data;
} LegacyEnumFilesCallbackData;

static PHYSFS_EnumerateCallbackResult enumFilesCallbackAlwaysSucceed(void *d,
                                    const char *origdir, const char *fname, const unsigned char dv)
{
    LegacyEnumFilesCallbackData *cbdata = (LegacyEnumFilesCallbackData *) d;
    cbdata->callback(cbdata->data, origdir, fname, dv);
    return PHYSFS_ENUM_OK;
} /* enumFilesCallbackAlwaysSucceed */

void PHYSFS_enumerateFilesCallback(const char *fname,
                                   PHYSFS_EnumFilesCallback callback,
                                   void *data, const unsigned char dv)
{
    LegacyEnumFilesCallbackData cbdata;
    cbdata.callback = callback;
    cbdata.data = data;
    (void) PHYSFS_enumerate(fname, enumFilesCallbackAlwaysSucceed, &cbdata, dv);
} /* PHYSFS_enumerateFilesCallback */


int PHYSFS_exists(const char *fname, const unsigned char dv)
{
    return (getRealDirHandle(fname, dv) != NULL);
} /* PHYSFS_exists */


PHYSFS_sint64 PHYSFS_getLastModTime(const char *fname, const unsigned char dv)
{
    PHYSFS_Stat statbuf;
    BAIL_IF_ERRPASS(!PHYSFS_stat(fname, &statbuf, dv), -1, dv);
    return statbuf.modtime;
} /* PHYSFS_getLastModTime */


int PHYSFS_isDirectory(const char *fname, const unsigned char dv)
{
    PHYSFS_Stat statbuf;
    BAIL_IF_ERRPASS(!PHYSFS_stat(fname, &statbuf, dv), 0, dv);
    return (statbuf.filetype == PHYSFS_FILETYPE_DIRECTORY);
} /* PHYSFS_isDirectory */


int PHYSFS_isSymbolicLink(const char *fname, const unsigned char dv)
{
    PHYSFS_Stat statbuf;
    BAIL_IF_ERRPASS(!PHYSFS_stat(fname, &statbuf, dv), 0, dv);
    return (statbuf.filetype == PHYSFS_FILETYPE_SYMLINK);
} /* PHYSFS_isSymbolicLink */


static PHYSFS_File *doOpenWrite(const char *_fname, int appending, const unsigned char dv)
{
    FileHandle *fh = NULL;
    size_t len;
    char *fname;

    BAIL_IF(!_fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    len = strlen(_fname) + 1;
    fname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);

    if (sanitizePlatformIndependentPath(_fname, fname, dv))
    {
        PHYSFS_Io *io = NULL;
        DirHandle *h = NULL;
        const PHYSFS_Archiver *f;

        __PHYSFS_platformGrabMutex(stateLock[dv]);

        GOTO_IF(!writeDir[dv], PHYSFS_ERR_NO_WRITE_DIR, doOpenWriteEnd, dv);

        h = writeDir[dv];
        GOTO_IF_ERRPASS(!verifyPath(h, &fname, 0, dv), doOpenWriteEnd, dv);

        f = h->funcs;
        if (appending)
            io = f->openAppend(h->opaque, fname, dv);
        else
            io = f->openWrite(h->opaque, fname, dv);

        GOTO_IF_ERRPASS(!io, doOpenWriteEnd, dv);

        fh = (FileHandle *) allocator[dv].Malloc(sizeof (FileHandle), dv);
        if (fh == NULL)
        {
            io->destroy(io, dv);
            GOTO(PHYSFS_ERR_OUT_OF_MEMORY, doOpenWriteEnd, dv);
        } /* if */
        else
        {
            memset(fh, '\0', sizeof (FileHandle));
            fh->io = io;
            fh->dirHandle = h;
            fh->next = openWriteList[dv];
            openWriteList[dv] = fh;
        } /* else */

        doOpenWriteEnd:
        __PHYSFS_platformReleaseMutex(stateLock[dv]);
    } /* if */

    __PHYSFS_smallFree(fname, dv);
    return ((PHYSFS_File *) fh);
} /* doOpenWrite */


PHYSFS_File *PHYSFS_openWrite(const char *filename, const unsigned char dv)
{
    return doOpenWrite(filename, 0, dv);
} /* PHYSFS_openWrite */


PHYSFS_File *PHYSFS_openAppend(const char *filename, const unsigned char dv)
{
    return doOpenWrite(filename, 1, dv);
} /* PHYSFS_openAppend */


PHYSFS_File *PHYSFS_openRead(const char *_fname, const unsigned char dv)
{
    FileHandle *fh = NULL;
    char *fname;
    size_t len;

    BAIL_IF(!_fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    len = strlen(_fname) + 1;
    fname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);

    if (sanitizePlatformIndependentPath(_fname, fname, dv))
    {
        DirHandle *i = NULL;
        PHYSFS_Io *io = NULL;

        __PHYSFS_platformGrabMutex(stateLock[dv]);

        GOTO_IF(!searchPath[dv], PHYSFS_ERR_NOT_FOUND, openReadEnd, dv);

        for (i = searchPath[dv]; i != NULL; i = i->next)
        {
            char *arcfname = fname;
            if (verifyPath(i, &arcfname, 0, dv))
            {
                io = i->funcs->openRead(i->opaque, arcfname, dv);
                if (io)
                    break;
            } /* if */
        } /* for */

        GOTO_IF_ERRPASS(!io, openReadEnd, dv);

        fh = (FileHandle *) allocator[dv].Malloc(sizeof (FileHandle), dv);
        if (fh == NULL)
        {
            io->destroy(io, dv);
            GOTO(PHYSFS_ERR_OUT_OF_MEMORY, openReadEnd, dv);
        } /* if */

        memset(fh, '\0', sizeof (FileHandle));
        fh->io = io;
        fh->forReading = 1;
        fh->dirHandle = i;
        fh->next = openReadList[dv];
        openReadList[dv] = fh;

        openReadEnd:
        __PHYSFS_platformReleaseMutex(stateLock[dv]);
    } /* if */

    __PHYSFS_smallFree(fname, dv);
    return ((PHYSFS_File *) fh);
} /* PHYSFS_openRead */


static int closeHandleInOpenList(FileHandle **list, FileHandle *handle, const unsigned char dv)
{
    FileHandle *prev = NULL;
    FileHandle *i;

    for (i = *list; i != NULL; i = i->next)
    {
        if (i == handle)  /* handle is in this list? */
        {
            PHYSFS_Io *io = handle->io;
            PHYSFS_uint8 *tmp = handle->buffer;

            /* send our buffer to io... */
            if (!handle->forReading)
            {
                if (!PHYSFS_flush((PHYSFS_File *) handle, dv))
                    return -1;

                /* ...then have io send it to the disk... */
                else if (io->flush && !io->flush(io, dv))
                    return -1;
            } /* if */

            /* ...then close the underlying file. */
            io->destroy(io,dv);

            if (tmp != NULL)  /* free any associated buffer. */
                allocator[dv].Free(tmp, dv);

            if (prev == NULL)
                *list = handle->next;
            else
                prev->next = handle->next;

            allocator[dv].Free(handle, dv);
            return 1;
        } /* if */
        prev = i;
    } /* for */

    return 0;
} /* closeHandleInOpenList */


int PHYSFS_close(PHYSFS_File *_handle, const unsigned char dv)
{
    FileHandle *handle = (FileHandle *) _handle;
    int rc;

    __PHYSFS_platformGrabMutex(stateLock[dv]);

    /* -1 == close failure. 0 == not found. 1 == success. */
    rc = closeHandleInOpenList(&openReadList[dv], handle, dv);
    BAIL_IF_MUTEX_ERRPASS(rc == -1, stateLock[dv], 0, dv);
    if (!rc)
    {
        rc = closeHandleInOpenList(&openWriteList[dv], handle, dv);
        BAIL_IF_MUTEX_ERRPASS(rc == -1, stateLock[dv], 0, dv);
    } /* if */

    __PHYSFS_platformReleaseMutex(stateLock[dv]);
    BAIL_IF(!rc, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    return 1;
} /* PHYSFS_close */


static PHYSFS_sint64 doBufferedRead(FileHandle *fh, void *_buffer, size_t len, const unsigned char dv)
{
    PHYSFS_uint8 *buffer = (PHYSFS_uint8 *) _buffer;
    PHYSFS_sint64 retval = 0;

    while (len > 0)
    {
        const size_t avail = fh->buffill - fh->bufpos;
        if (avail > 0)  /* data available in the buffer. */
        {
            const size_t cpy = (len < avail) ? len : avail;
            memcpy(buffer, fh->buffer + fh->bufpos, cpy);
            assert(len >= cpy);
            buffer += cpy;
            len -= cpy;
            fh->bufpos += cpy;
            retval += cpy;
        } /* if */

        else   /* buffer is empty, refill it. */
        {
            PHYSFS_Io *io = fh->io;
            const PHYSFS_sint64 rc = io->read(io, fh->buffer, fh->bufsize, dv);
            fh->bufpos = 0;
            if (rc > 0)
                fh->buffill = (size_t) rc;
            else
            {
                fh->buffill = 0;
                if (retval == 0)  /* report already-read data, or failure. */
                    retval = rc;
                break;
            } /* else */
        } /* else */
    } /* while */

    return retval;
} /* doBufferedRead */


PHYSFS_sint64 PHYSFS_read(PHYSFS_File *handle, void *buffer,
                          PHYSFS_uint32 size, PHYSFS_uint32 count, const unsigned char dv)
{
    const PHYSFS_uint64 len = ((PHYSFS_uint64) size) * ((PHYSFS_uint64) count);
    const PHYSFS_sint64 retval = PHYSFS_readBytes(handle, buffer, len, dv);
    return ( (retval <= 0) ? retval : (retval / ((PHYSFS_sint64) size)) );
} /* PHYSFS_read */


PHYSFS_sint64 PHYSFS_readBytes(PHYSFS_File *handle, void *buffer,
                               PHYSFS_uint64 _len, const unsigned char dv)
{
    const size_t len = (size_t) _len;
    FileHandle *fh = (FileHandle *) handle;

#ifdef PHYSFS_NO_64BIT_SUPPORT
    const PHYSFS_uint64 maxlen = __PHYSFS_UI64(0x7FFFFFFF);
#else
    const PHYSFS_uint64 maxlen = __PHYSFS_UI64(0x7FFFFFFFFFFFFFFF);
#endif

    if (!__PHYSFS_ui64FitsAddressSpace(_len))
        BAIL(PHYSFS_ERR_INVALID_ARGUMENT, -1, dv);

    BAIL_IF(_len > maxlen, PHYSFS_ERR_INVALID_ARGUMENT, -1, dv);
    BAIL_IF(!fh->forReading, PHYSFS_ERR_OPEN_FOR_WRITING, -1, dv);
    BAIL_IF_ERRPASS(len == 0, 0, dv);
    if (fh->buffer)
        return doBufferedRead(fh, buffer, len, dv);

    return fh->io->read(fh->io, buffer, len, dv);
} /* PHYSFS_readBytes */


static PHYSFS_sint64 doBufferedWrite(PHYSFS_File *handle, const void *buffer,
                                     const size_t len, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;

    /* whole thing fits in the buffer? */
    if ((fh->buffill + len) < fh->bufsize)
    {
        memcpy(fh->buffer + fh->buffill, buffer, len);
        fh->buffill += len;
        return (PHYSFS_sint64) len;
    } /* if */

    /* would overflow buffer. Flush and then write the new objects, too. */
    BAIL_IF_ERRPASS(!PHYSFS_flush(handle, dv), -1, dv);
    return fh->io->write(fh->io, buffer, len, dv);
} /* doBufferedWrite */


PHYSFS_sint64 PHYSFS_write(PHYSFS_File *handle, const void *buffer,
                           PHYSFS_uint32 size, PHYSFS_uint32 count, const unsigned char dv)
{
    const PHYSFS_uint64 len = ((PHYSFS_uint64) size) * ((PHYSFS_uint64) count);
    const PHYSFS_sint64 retval = PHYSFS_writeBytes(handle, buffer, len, dv);
    return ( (retval <= 0) ? retval : (retval / ((PHYSFS_sint64) size)) );
} /* PHYSFS_write */


PHYSFS_sint64 PHYSFS_writeBytes(PHYSFS_File *handle, const void *buffer,
                                PHYSFS_uint64 _len, const unsigned char dv)
{
    const size_t len = (size_t) _len;
    FileHandle *fh = (FileHandle *) handle;

#ifdef PHYSFS_NO_64BIT_SUPPORT
    const PHYSFS_uint64 maxlen = __PHYSFS_UI64(0x7FFFFFFF);
#else
    const PHYSFS_uint64 maxlen = __PHYSFS_UI64(0x7FFFFFFFFFFFFFFF);
#endif

    if (!__PHYSFS_ui64FitsAddressSpace(_len))
        BAIL(PHYSFS_ERR_INVALID_ARGUMENT, -1, dv);

    BAIL_IF(_len > maxlen, PHYSFS_ERR_INVALID_ARGUMENT, -1, dv);
    BAIL_IF(fh->forReading, PHYSFS_ERR_OPEN_FOR_READING, -1, dv);
    BAIL_IF_ERRPASS(len == 0, 0, dv);
    if (fh->buffer)
        return doBufferedWrite(handle, buffer, len, dv);

    return fh->io->write(fh->io, buffer, len, dv);
} /* PHYSFS_write */


int PHYSFS_eof(PHYSFS_File *handle, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;

    if (!fh->forReading)  /* never EOF on files opened for write/append. */
        return 0;

    /* can't be eof if buffer isn't empty */
    if (fh->bufpos == fh->buffill)
    {
        /* check the Io. */
        PHYSFS_Io *io = fh->io;
        const PHYSFS_sint64 pos = io->tell(io, dv);
        const PHYSFS_sint64 len = io->length(io, dv);
        if ((pos < 0) || (len < 0))
            return 0;  /* beats me. */
        return (pos >= len);
    } /* if */

    return 0;
} /* PHYSFS_eof */


PHYSFS_sint64 PHYSFS_tell(PHYSFS_File *handle, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;
    const PHYSFS_sint64 pos = fh->io->tell(fh->io, dv);
    const PHYSFS_sint64 retval = fh->forReading ?
                                 (pos - fh->buffill) + fh->bufpos :
                                 (pos + fh->buffill);
    return retval;
} /* PHYSFS_tell */


int PHYSFS_seek(PHYSFS_File *handle, PHYSFS_uint64 pos, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;
    BAIL_IF_ERRPASS(!PHYSFS_flush(handle, dv), 0, dv);

    if (fh->buffer && fh->forReading)
    {
        /* avoid throwing away our precious buffer if seeking within it. */
        PHYSFS_sint64 offset = pos - PHYSFS_tell(handle, dv);
        if ( /* seeking within the already-buffered range? */
             /* forward? */
            ((offset >= 0) && (((size_t)offset) <= fh->buffill-fh->bufpos)) ||
            /* backward? */
            ((offset < 0) && (((size_t) -offset) <= fh->bufpos)) )
        {
            fh->bufpos = (size_t) (((PHYSFS_sint64) fh->bufpos) + offset);
            return 1; /* successful seek */
        } /* if */
    } /* if */

    /* we have to fall back to a 'raw' seek. */
    fh->buffill = fh->bufpos = 0;
    return fh->io->seek(fh->io, pos, dv);
} /* PHYSFS_seek */


PHYSFS_sint64 PHYSFS_fileLength(PHYSFS_File *handle, const unsigned char dv)
{
    PHYSFS_Io *io = ((FileHandle *) handle)->io;
    return io->length(io, dv);
} /* PHYSFS_filelength */


int PHYSFS_setBuffer(PHYSFS_File *handle, PHYSFS_uint64 _bufsize, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;
    const size_t bufsize = (size_t) _bufsize;

    if (!__PHYSFS_ui64FitsAddressSpace(_bufsize))
        BAIL(PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);

    BAIL_IF_ERRPASS(!PHYSFS_flush(handle, dv), 0, dv);

    /*
     * For reads, we need to move the file pointer to where it would be
     *  if we weren't buffering, so that the next read will get the
     *  right chunk of stuff from the file. PHYSFS_flush() handles writes.
     */
    if ((fh->forReading) && (fh->buffill != fh->bufpos))
    {
        PHYSFS_uint64 pos;
        const PHYSFS_sint64 curpos = fh->io->tell(fh->io, dv);
        BAIL_IF_ERRPASS(curpos == -1, 0, dv);
        pos = ((curpos - fh->buffill) + fh->bufpos);
        BAIL_IF_ERRPASS(!fh->io->seek(fh->io, pos, dv), 0, dv);
    } /* if */

    if (bufsize == 0)  /* delete existing buffer. */
    {
        if (fh->buffer)
        {
            allocator[dv].Free(fh->buffer, dv);
            fh->buffer = NULL;
        } /* if */
    } /* if */

    else
    {
        PHYSFS_uint8 *newbuf;
        newbuf = (PHYSFS_uint8 *) allocator[dv].Realloc(fh->buffer, bufsize, dv);
        BAIL_IF(!newbuf, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);
        fh->buffer = newbuf;
    } /* else */

    fh->bufsize = bufsize;
    fh->buffill = fh->bufpos = 0;
    return 1;
} /* PHYSFS_setBuffer */


int PHYSFS_flush(PHYSFS_File *handle, const unsigned char dv)
{
    FileHandle *fh = (FileHandle *) handle;
    PHYSFS_Io *io;
    PHYSFS_sint64 rc;

    if ((fh->forReading) || (fh->bufpos == fh->buffill))
        return 1;  /* open for read or buffer empty are successful no-ops. */

    /* dump buffer to disk. */
    io = fh->io;
    rc = io->write(io, fh->buffer + fh->bufpos, fh->buffill - fh->bufpos, dv);
    BAIL_IF_ERRPASS(rc <= 0, 0, dv);
    fh->bufpos = fh->buffill = 0;
    return 1;
} /* PHYSFS_flush */


int PHYSFS_stat(const char *_fname, PHYSFS_Stat *stat, const unsigned char dv)
{
    int retval = 0;
    char *fname;
    size_t len;

    BAIL_IF(!_fname, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    BAIL_IF(!stat, PHYSFS_ERR_INVALID_ARGUMENT, 0, dv);
    len = strlen(_fname) + 1;
    fname = (char *) __PHYSFS_smallAlloc(len,dv);
    BAIL_IF(!fname, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);

    /* set some sane defaults... */
    stat->filesize = -1;
    stat->modtime = -1;
    stat->createtime = -1;
    stat->accesstime = -1;
    stat->filetype = PHYSFS_FILETYPE_OTHER;
    stat->readonly = 1;

    if (sanitizePlatformIndependentPath(_fname, fname, dv))
    {
        if (*fname == '\0')
        {
            stat->filetype = PHYSFS_FILETYPE_DIRECTORY;
            stat->readonly = !writeDir[dv]; /* Writeable if we have a writeDir */
            retval = 1;
        } /* if */
        else
        {
            DirHandle *i;
            int exists = 0;
            __PHYSFS_platformGrabMutex(stateLock[dv]);
            for (i = searchPath[dv]; ((i != NULL) && (!exists)); i = i->next)
            {
                char *arcfname = fname;
                exists = partOfMountPoint(i, arcfname, dv);
                if (exists)
                {
                    stat->filetype = PHYSFS_FILETYPE_DIRECTORY;
                    stat->readonly = 1;
                    retval = 1;
                } /* if */
                else if (verifyPath(i, &arcfname, 0, dv))
                {
                    retval = i->funcs->stat(i->opaque, arcfname, stat, dv);
                    if ((retval) || (currentErrorCode(dv) != PHYSFS_ERR_NOT_FOUND))
                        exists = 1;
                } /* else if */
            } /* for */
            __PHYSFS_platformReleaseMutex(stateLock[dv]);
        } /* else */
    } /* if */

    __PHYSFS_smallFree(fname, dv);
    return retval;
} /* PHYSFS_stat */


int __PHYSFS_readAll(PHYSFS_Io *io, void *buf, const size_t _len, const unsigned char dv)
{
    const PHYSFS_uint64 len = (PHYSFS_uint64) _len;
    return (io->read(io, buf, len, dv) == len);
} /* __PHYSFS_readAll */


void *__PHYSFS_initSmallAlloc(void *ptr, const size_t len, const unsigned char dv)
{
    void *useHeap = ((ptr == NULL) ? ((void *) 1) : ((void *) 0));
    if (useHeap)  /* too large for stack allocation or alloca() failed. */
        ptr = allocator[dv].Malloc(len+sizeof (void *), dv);

    if (ptr != NULL)
    {
        void **retval = (void **) ptr;
        /*printf("%s alloc'd (%lld) bytes at (%p).\n",
                useHeap ? "heap" : "stack", (long long) len, ptr);*/
        *retval = useHeap;
        return retval + 1;
    } /* if */

    return NULL;  /* allocation failed. */
} /* __PHYSFS_initSmallAlloc */


void __PHYSFS_smallFree(void *ptr, const unsigned char dv)
{
    if (ptr != NULL)
    {
        void **block = ((void **) ptr) - 1;
        const int useHeap = (*block != NULL);
        if (useHeap)
            allocator[dv].Free(block, dv);
        /*printf("%s free'd (%p).\n", useHeap ? "heap" : "stack", block);*/
    } /* if */
} /* __PHYSFS_smallFree */


int PHYSFS_setAllocator(const PHYSFS_Allocator *a, const unsigned char dv)
{
    BAIL_IF(initialized[dv], PHYSFS_ERR_IS_INITIALIZED, 0, dv);
    externalAllocator[dv] = (a != NULL);
    if (externalAllocator[dv])
        memcpy(&allocator[dv], a, sizeof (PHYSFS_Allocator));

    return 1;
} /* PHYSFS_setAllocator */


const PHYSFS_Allocator *PHYSFS_getAllocator(const unsigned char dv)
{
    BAIL_IF(!initialized[dv], PHYSFS_ERR_NOT_INITIALIZED, NULL, dv);
    return &allocator[dv];
} /* PHYSFS_getAllocator */


static void *mallocAllocatorMalloc(PHYSFS_uint64 s, const unsigned char dv)
{
    if (!__PHYSFS_ui64FitsAddressSpace(s))
        BAIL(PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    #undef malloc
    return malloc((size_t) s);
} /* mallocAllocatorMalloc */


static void *mallocAllocatorRealloc(void *ptr, PHYSFS_uint64 s, const unsigned char dv)
{
    if (!__PHYSFS_ui64FitsAddressSpace(s))
        BAIL(PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    #undef realloc
    return realloc(ptr, (size_t) s);
} /* mallocAllocatorRealloc */


static void mallocAllocatorFree(void *ptr, const unsigned char dv)
{
    #undef free
    free(ptr);
} /* mallocAllocatorFree */


static void setDefaultAllocator(const unsigned char dv)
{
    assert(!externalAllocator[dv]);
    allocator[dv].Init = NULL;
    allocator[dv].Deinit = NULL;
    allocator[dv].Malloc = mallocAllocatorMalloc;
    allocator[dv].Realloc = mallocAllocatorRealloc;
    allocator[dv].Free = mallocAllocatorFree;
} /* setDefaultAllocator */


int __PHYSFS_DirTreeInit(__PHYSFS_DirTree *dt, const size_t entrylen, const unsigned char dv)
{
    static char rootpath[2] = { '/', '\0' };
    size_t alloclen;

    assert(entrylen >= sizeof (__PHYSFS_DirTreeEntry));

    memset(dt, '\0', sizeof (*dt));

    dt->root = (__PHYSFS_DirTreeEntry *) allocator[dv].Malloc(entrylen, dv);
    BAIL_IF(!dt->root, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);
    memset(dt->root, '\0', entrylen);
    dt->root->name = rootpath;
    dt->root->isdir = 1;
    dt->hashBuckets = 64;
    if (!dt->hashBuckets)
        dt->hashBuckets = 1;
    dt->entrylen = entrylen;

    alloclen = dt->hashBuckets * sizeof (__PHYSFS_DirTreeEntry *);
    dt->hash = (__PHYSFS_DirTreeEntry **) allocator[dv].Malloc(alloclen, dv);
    BAIL_IF(!dt->hash, PHYSFS_ERR_OUT_OF_MEMORY, 0, dv);
    memset(dt->hash, '\0', alloclen);

    return 1;
} /* __PHYSFS_DirTreeInit */


static inline PHYSFS_uint32 hashPathName(__PHYSFS_DirTree *dt, const char *name, const unsigned char dv)
{
    return __PHYSFS_hashString(name, strlen(name)) % dt->hashBuckets;
} /* hashPathName */


/* Fill in missing parent directories. */
static __PHYSFS_DirTreeEntry *addAncestors(__PHYSFS_DirTree *dt, char *name, const unsigned char dv)
{
    __PHYSFS_DirTreeEntry *retval = dt->root;
    char *sep = strrchr(name, '/');

    if (sep)
    {
        *sep = '\0';  /* chop off last piece. */
        retval = (__PHYSFS_DirTreeEntry *) __PHYSFS_DirTreeFind(dt, name, dv);

        if (retval != NULL)
        {
            *sep = '/';
            BAIL_IF(!retval->isdir, PHYSFS_ERR_CORRUPT, NULL, dv);
            return retval;  /* already hashed. */
        } /* if */

        /* okay, this is a new dir. Build and hash us. */
        retval = (__PHYSFS_DirTreeEntry*)__PHYSFS_DirTreeAdd(dt, name, 1, dv);
        *sep = '/';
    } /* if */

    return retval;
} /* addAncestors */


void *__PHYSFS_DirTreeAdd(__PHYSFS_DirTree *dt, char *name, const int isdir, const unsigned char dv)
{
    __PHYSFS_DirTreeEntry *retval = __PHYSFS_DirTreeFind(dt, name, dv);
    if (!retval)
    {
        const size_t alloclen = strlen(name) + 1 + dt->entrylen;
        PHYSFS_uint32 hashval;
        __PHYSFS_DirTreeEntry *parent = addAncestors(dt, name, dv);
        BAIL_IF_ERRPASS(!parent, NULL, dv);
        assert(dt->entrylen >= sizeof (__PHYSFS_DirTreeEntry));
        retval = (__PHYSFS_DirTreeEntry *) allocator[dv].Malloc(alloclen, dv);
        BAIL_IF(!retval, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
        memset(retval, '\0', dt->entrylen);
        retval->name = ((char *) retval) + dt->entrylen;
        strcpy(retval->name, name);
        hashval = hashPathName(dt, name, dv);
        retval->hashnext = dt->hash[hashval];
        dt->hash[hashval] = retval;
        retval->sibling = parent->children;
        retval->isdir = isdir;
        parent->children = retval;
    } /* if */

    return retval;
} /* __PHYSFS_DirTreeAdd */


/* Find the __PHYSFS_DirTreeEntry for a path in platform-independent notation. */
void *__PHYSFS_DirTreeFind(__PHYSFS_DirTree *dt, const char *path, const unsigned char dv)
{
    PHYSFS_uint32 hashval;
    __PHYSFS_DirTreeEntry *prev = NULL;
    __PHYSFS_DirTreeEntry *retval;

    if (*path == '\0')
        return dt->root;

    hashval = hashPathName(dt, path, dv);
    for (retval = dt->hash[hashval]; retval; retval = retval->hashnext)
    {
        if (strcmp(retval->name, path) == 0)
        {
            if (prev != NULL)  /* move this to the front of the list */
            {
                prev->hashnext = retval->hashnext;
                retval->hashnext = dt->hash[hashval];
                dt->hash[hashval] = retval;
            } /* if */

            return retval;
        } /* if */

        prev = retval;
    } /* for */

    BAIL(PHYSFS_ERR_NOT_FOUND, NULL, dv);
} /* __PHYSFS_DirTreeFind */

PHYSFS_EnumerateCallbackResult __PHYSFS_DirTreeEnumerate(void *opaque,
                              const char *dname, PHYSFS_EnumerateCallback cb,
                              const char *origdir, void *callbackdata, const unsigned char dv)
{
    PHYSFS_EnumerateCallbackResult retval = PHYSFS_ENUM_OK;
    __PHYSFS_DirTree *tree = (__PHYSFS_DirTree *) opaque;
    const __PHYSFS_DirTreeEntry *entry = __PHYSFS_DirTreeFind(tree, dname, dv);
    BAIL_IF(!entry, PHYSFS_ERR_NOT_FOUND, PHYSFS_ENUM_ERROR, dv);

    entry = entry->children;

    while (entry && (retval == PHYSFS_ENUM_OK))
    {
        const char *name = entry->name;
        const char *ptr = strrchr(name, '/');
        retval = cb(callbackdata, origdir, ptr ? ptr + 1 : name, dv);
        BAIL_IF(retval == PHYSFS_ENUM_ERROR, PHYSFS_ERR_APP_CALLBACK, retval, dv);
        entry = entry->sibling;
    } /* while */

    return retval;
} /* __PHYSFS_DirTreeEnumerate */


void __PHYSFS_DirTreeDeinit(__PHYSFS_DirTree *dt, const unsigned char dv)
{
    if (!dt)
        return;

    if (dt->root)
    {
        assert(dt->root->sibling == NULL);
        assert(dt->hash || (dt->root->children == NULL));
        allocator[dv].Free(dt->root, dv);
    } /* if */

    if (dt->hash)
    {
        size_t i;
        for (i = 0; i < dt->hashBuckets; i++)
        {
            __PHYSFS_DirTreeEntry *entry;
            __PHYSFS_DirTreeEntry *next;
            for (entry = dt->hash[i]; entry; entry = next)
            {
                next = entry->hashnext;
                allocator[dv].Free(entry, dv);
            } /* for */
        } /* for */
        allocator[dv].Free(dt->hash, dv);
    } /* if */
} /* __PHYSFS_DirTreeDeinit */

/* end of physfs.c ... */
