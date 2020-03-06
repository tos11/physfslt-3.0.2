/*
 * Linux support routines for PhysicsFS.
 *
 * Please see the file LICENSE.txt in the source's root directory.
 *
 *  This file written by Ryan C. Gordon.
 */

#define __PHYSICSFS_INTERNAL__
#include "physfs_platforms.h"

#ifdef PHYSFS_PLATFORM_UNIX

#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <dirent.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#define PHYSFS_HAVE_MNTENT_H 1

#ifdef PHYSFS_HAVE_SYS_UCRED_H
#  ifdef PHYSFS_HAVE_MNTENT_H
#    undef PHYSFS_HAVE_MNTENT_H /* don't do both... */
#  endif
#  include <sys/mount.h>
#  include <sys/ucred.h>
#endif

#ifdef PHYSFS_HAVE_MNTENT_H
#include <mntent.h>
#endif

#include "physfs_internal.h"

/*
 * See where program (bin) resides in the $PATH specified by (envr).
 *  returns a copy of the first element in envr that contains it, or NULL
 *  if it doesn't exist or there were other problems. PHYSFS_SetError() is
 *  called if we have a problem.
 *
 * (envr) will be scribbled over, and you are expected to allocator.Free() the
 *  return value when you're done with it.
 */
static char *findBinaryInPath(const char *bin, char *envr, const unsigned char dv)
{
    size_t alloc_size = 0;
    char *exe = NULL;
    char *start = envr;
    char *ptr;

    assert(bin != NULL);
    assert(envr != NULL);

    do
    {
        size_t size;
        size_t binlen;

        ptr = strchr(start, ':');  /* find next $PATH separator. */
        if (ptr)
            *ptr = '\0';

        binlen = strlen(bin);
        size = strlen(start) + binlen + 2;
        if (size >= alloc_size)
        {
            char *x = (char *) allocator[dv].Realloc(exe, size, dv);
            if (!x)
            {
                if (exe != NULL)
                    allocator[dv].Free(exe, dv);
                BAIL(PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
            } /* if */

            alloc_size = size;
            exe = x;
        } /* if */

        /* build full binary path... */
        strcpy(exe, start);
        if ((exe[0] == '\0') || (exe[strlen(exe) - 1] != '/'))
            strcat(exe, "/");
        strcat(exe, bin);

        if (access(exe, X_OK) == 0)  /* Exists as executable? We're done. */
        {
            exe[(size - binlen) - 1] = '\0'; /* chop off filename, leave '/' */
            return exe;
        } /* if */

        start = ptr + 1;  /* start points to beginning of next element. */
    } while (ptr != NULL);

    if (exe != NULL)
        allocator[dv].Free(exe, dv);

    return NULL;  /* doesn't exist in path. */
} /* findBinaryInPath */


static char *readSymLink(const char *path, const unsigned char dv)
{
    ssize_t len = 64;
    ssize_t rc = -1;
    char *retval = NULL;

    while (1)
    {
         char *ptr = (char *) allocator[dv].Realloc(retval, (size_t) len, dv);
         if (ptr == NULL)
             break;   /* out of memory. */
         retval = ptr;

         rc = readlink(path, retval, len);
         if (rc == -1)
             break;  /* not a symlink, i/o error, etc. */

         else if (rc < len)
         {
             retval[rc] = '\0';  /* readlink doesn't null-terminate. */
             return retval;  /* we're good to go. */
         } /* else if */

         len *= 2;  /* grow buffer, try again. */
    } /* while */

    if (retval != NULL)
        allocator[dv].Free(retval, dv);
    return NULL;
} /* readSymLink */


char *__PHYSFS_platformCalcBaseDir(const char *argv0, const unsigned char dv)
{
    char *retval = NULL;
    const char *envr = NULL;

    /* Try to avoid using argv0 unless forced to. Try system-specific stuff. */

    /* If there's a Linux-like /proc filesystem, you can get the full path to
     *  the current process from a symlink in there.
     */

    if (!retval && (access("/proc", F_OK) == 0))
    {
        retval = readSymLink("/proc/self/exe", dv);
        if (!retval) retval = readSymLink("/proc/curproc/file", dv);
        if (!retval) retval = readSymLink("/proc/curproc/exe", dv);
        if (retval == NULL)
        {
            /* older kernels don't have /proc/self ... try PID version... */
            const unsigned long long pid = (unsigned long long) getpid();
            char path[64];
            const int rc = (int) snprintf(path,sizeof(path),"/proc/%llu/exe",pid);
            if ( (rc > 0) && (rc < sizeof(path)) )
                retval = readSymLink(path, dv);
        } /* if */
    } /* if */

    if (retval != NULL)  /* chop off filename. */
    {
        char *ptr = strrchr(retval, '/');
        if (ptr != NULL)
            *(ptr+1) = '\0';
        else  /* shouldn't happen, but just in case... */
        {
            allocator[dv].Free(retval, dv);
            retval = NULL;
        } /* else */
    } /* if */

    /* No /proc/self/exe, etc, but we have an argv[0] we can parse? */
    if ((retval == NULL) && (argv0 != NULL))
    {
        /* fast path: default behaviour can handle this. */
        if (strchr(argv0, '/') != NULL)
            return NULL;  /* higher level parses out real path from argv0. */

        /* If there's no dirsep on argv0, then look through $PATH for it. */
        envr = getenv("PATH");
        if (envr != NULL)
        {
            char *path = (char *) __PHYSFS_smallAlloc(strlen(envr) + 1, dv);
            BAIL_IF(!path, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
            strcpy(path, envr);
            retval = findBinaryInPath(argv0, path, dv);
            __PHYSFS_smallFree(path, dv);
        } /* if */
    } /* if */

    if (retval != NULL)
    {
        /* try to shrink buffer... */
        char *ptr = (char *) allocator[dv].Realloc(retval, strlen(retval) + 1, dv);
        if (ptr != NULL)
            retval = ptr;  /* oh well if it failed. */
    } /* if */

    return retval;
} /* __PHYSFS_platformCalcBaseDir */


char *__PHYSFS_platformCalcPrefDir(const char *org, const char *app, const unsigned char dv)
{
    /*
     * We use XDG's base directory spec, even if you're not on Linux.
     *  This isn't strictly correct, but the results are relatively sane
     *  in any case.
     *
     * https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
     */
    const char *envr = getenv("XDG_DATA_HOME");
    const char *append = "/";
    char *retval = NULL;
    size_t len = 0;

    if (!envr)
    {
        /* You end up with "$HOME/.local/share/Game Name 2" */
        envr = __PHYSFS_getUserDir(dv);
        BAIL_IF_ERRPASS(!envr, NULL, dv);  /* oh well. */
        append = ".local/share/";
    } /* if */

    len = strlen(envr) + strlen(append) + strlen(app) + 2;
    retval = (char *) allocator[dv].Malloc(len, dv);
    BAIL_IF(!retval, PHYSFS_ERR_OUT_OF_MEMORY, NULL, dv);
    snprintf(retval, len, "%s%s%s/", envr, append, app);
    return retval;
} /* __PHYSFS_platformCalcPrefDir */

#endif /* PHYSFS_PLATFORM_UNIX */

/* end of physfs_platform_unix.c ... */

