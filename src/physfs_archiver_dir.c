/*
 * Standard directory I/O support routines for PhysicsFS.
 *
 * Please see the file LICENSE.txt in the source's root directory.
 *
 *  This file written by Ryan C. Gordon.
 */

#define __PHYSICSFS_INTERNAL__
#include "physfs_internal.h"

/* There's no PHYSFS_Io interface here. Use __PHYSFS_createNativeIo(). */



static char *cvtToDependent(const char *prepend, const char *path,
                            char *buf, const size_t buflen, const unsigned char dv)
{
    BAIL_IF(buf == NULL, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    snprintf(buf, buflen, "%s%s", prepend ? prepend : "", path);

    #if !__PHYSFS_STANDARD_DIRSEP
    assert(__PHYSFS_platformDirSeparator != '/');
    {
        char *p;
        for (p = strchr(buf, '/'); p != NULL; p = strchr(p + 1, '/'))
            *p = __PHYSFS_platformDirSeparator;
    } /* if */
    #endif

    return buf;
} /* cvtToDependent */


#define CVT_TO_DEPENDENT(buf, pre, dir, dv) { \
    const size_t len = ((pre) ? strlen((char *) pre) : 0) + strlen(dir) + 1; \
    buf = cvtToDependent((char*)pre,dir,(char*)__PHYSFS_smallAlloc(len, dv),len, dv); \
}



static void *DIR_openArchive(PHYSFS_Io *io, const char *name,
                             int forWriting, int *claimed, const unsigned char dv)
{
    PHYSFS_Stat st;
    const char dirsep = __PHYSFS_platformDirSeparator;
    char *retval = NULL;
    const size_t namelen = strlen(name);
    const size_t seplen = 1;

    assert(io == NULL);  /* shouldn't create an Io for these. */
    BAIL_IF_ERRPASS(!__PHYSFS_platformStat(name, &st, 1, dv), NULL, dv);

    if (st.filetype != PHYSFS_FILETYPE_DIRECTORY)
        BAIL(PHYSFS_ERR_UNSUPPORTED, NULL, dv);

    *claimed = 1;
    retval = allocator[dv].Malloc(namelen + seplen + 1, dv);
    BAIL_IF(retval == NULL, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);

    strcpy(retval, name);

    /* make sure there's a dir separator at the end of the string */
    if (retval[namelen - 1] != dirsep)
    {
        retval[namelen] = dirsep;
        retval[namelen + 1] = '\0';
    } /* if */

    return retval;
} /* DIR_openArchive */


static PHYSFS_EnumerateCallbackResult DIR_enumerate(void *opaque,
                         const char *dname, PHYSFS_EnumerateCallback cb,
                         const char *origdir, void *callbackdata, const unsigned char dv)
{
    char *d;
    PHYSFS_EnumerateCallbackResult retval;
    CVT_TO_DEPENDENT(d, opaque, dname, dv);
    BAIL_IF_ERRPASS(!d, PHYSFS_ENUM_ERROR, dv);
    retval = __PHYSFS_platformEnumerate(d, cb, origdir, callbackdata, dv);
    __PHYSFS_smallFree(d, dv);
    return retval;
} /* DIR_enumerate */


static PHYSFS_Io *doOpen(void *opaque, const char *name, const int mode, const unsigned char dv)
{
    PHYSFS_Io *io = NULL;
    char *f = NULL;

    CVT_TO_DEPENDENT(f, opaque, name, dv);
    BAIL_IF_ERRPASS(!f, NULL, dv);

    io = __PHYSFS_createNativeIo(f, mode, dv);
    if (io == NULL)
    {
        const PHYSFS_ErrorCode err = PHYSFS_getLastErrorCode(dv);
        PHYSFS_Stat statbuf;
        __PHYSFS_platformStat(f, &statbuf, 0, dv);  /* !!! FIXME: why are we stating here? */
        PHYSFS_setErrorCode(err, dv);
    } /* if */

    __PHYSFS_smallFree(f, dv);

    return io;
} /* doOpen */


static PHYSFS_Io *DIR_openRead(void *opaque, const char *filename, const unsigned char dv)
{
    return doOpen(opaque, filename, 'r', dv);
} /* DIR_openRead */


static PHYSFS_Io *DIR_openWrite(void *opaque, const char *filename, const unsigned char dv)
{
    return doOpen(opaque, filename, 'w', dv);
} /* DIR_openWrite */


static PHYSFS_Io *DIR_openAppend(void *opaque, const char *filename, const unsigned char dv)
{
    return doOpen(opaque, filename, 'a', dv);
} /* DIR_openAppend */


static int DIR_remove(void *opaque, const char *name, const unsigned char dv)
{
    int retval;
    char *f;

    CVT_TO_DEPENDENT(f, opaque, name, dv);
    BAIL_IF_ERRPASS(!f, 0,dv);
    retval = __PHYSFS_platformDelete(f, dv);
    __PHYSFS_smallFree(f,dv);
    return retval;
} /* DIR_remove */


static int DIR_mkdir(void *opaque, const char *name, const unsigned char dv)
{
    int retval;
    char *f;

    CVT_TO_DEPENDENT(f, opaque, name, dv);
    BAIL_IF_ERRPASS(!f, 0,dv);
    retval = __PHYSFS_platformMkDir(f,dv);
    __PHYSFS_smallFree(f,dv);
    return retval;
} /* DIR_mkdir */


static void DIR_closeArchive(void *opaque, const unsigned char dv)
{
    allocator[dv].Free(opaque, dv);
} /* DIR_closeArchive */


static int DIR_stat(void *opaque, const char *name, PHYSFS_Stat *stat, const unsigned char dv)
{
    int retval = 0;
    char *d;

    CVT_TO_DEPENDENT(d, opaque, name, dv);
    BAIL_IF_ERRPASS(!d, 0, dv);
    retval = __PHYSFS_platformStat(d, stat, 0, dv);
    __PHYSFS_smallFree(d, dv);
    return retval;
} /* DIR_stat */


const PHYSFS_Archiver __PHYSFS_Archiver_DIR =
{
    CURRENT_PHYSFS_ARCHIVER_API_VERSION,
    {
        "",
        "Non-archive, direct filesystem I/O",
        "Ryan C. Gordon <icculus@icculus.org>",
        "https://icculus.org/physfs/",
        1,  /* supportsSymlinks */
    },
    DIR_openArchive,
    DIR_enumerate,
    DIR_openRead,
    DIR_openWrite,
    DIR_openAppend,
    DIR_remove,
    DIR_mkdir,
    DIR_stat,
    DIR_closeArchive
};

/* end of physfs_archiver_dir.c ... */

