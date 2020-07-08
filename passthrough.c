/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file
 *
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. Its performance is terrible.
 *
 * Compile with
 *
 *     gcc -Wall passthrough.c `pkg-config fuse3 --cflags --libs` -o passthrough
 *
 * ## Source code ##
 * \include passthrough.c
 */


#define FUSE_USE_VERSION 26

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

// popen2
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// popen2
#define READ 0
#define WRITE 1

// sb
#include "sb.h"

void pf(char* s) {
     FILE *fptr = fopen("/tmp/log.txt", "w");
     if (fptr == NULL)
     {
          printf("Could not open file");
          return;
     }

     fprintf(fptr,"%s\n", s);
     fclose(fptr);
}

// popen2
pid_t popen2(const char *command, int *infp, int *outfp)
{
     int p_stdin[2], p_stdout[2];
     pid_t pid;

     if (pipe(p_stdin) != 0 || pipe(p_stdout) != 0)
          return -1;

     pid = fork();
     if (pid < 0)
          return pid;
     else if (pid == 0)
     {
          close(p_stdin[WRITE]);
          dup2(p_stdin[READ], READ);
          close(p_stdout[READ]);
          dup2(p_stdout[WRITE], WRITE);
          execl("/bin/sh", "sh", "-c", command, NULL);
          perror("execl");
          exit(1);
     }

     if (infp == NULL)
          close(p_stdin[WRITE]);
     else
          *infp = p_stdin[WRITE];
     if (outfp == NULL)
          close(p_stdout[READ]);
     else
          *outfp = p_stdout[READ];
     return pid;
}

static void *xmp_init(struct fuse_conn_info *conn)
{
     (void) conn;
     //cfg->use_ino = 1;

     /* Pick up changes from lower filesystem right away. This is
        also necessary for better hardlink support. When the kernel
        calls the unlink() handler, it does not know the inode of
        the to-be-removed entry and can therefore not invalidate
        the cache of the associated inode - resulting in an
        incorrect st_nlink value being reported for any remaining
        hardlinks to this inode. */
     //cfg->entry_timeout = 0;
     //cfg->attr_timeout = 0;
     //cfg->negative_timeout = 0;

     return NULL;
}

static int xmp_getattr(const char *path, struct stat *stbuf)
{
     int res;

     res = lstat(path, stbuf);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_access(const char *path, int mask)
{
     int res;

     res = access(path, mask);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_readlink(const char *path, char *buf, size_t size)
{
     int res;

     res = readlink(path, buf, size - 1);
     if (res == -1)
          return -errno;

     buf[res] = '\0';
     return 0;
}


static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
     DIR *dp;
     struct dirent *de;

     (void) offset;
     (void) fi;

     dp = opendir(path);
     if (dp == NULL)
          return -errno;

     while ((de = readdir(dp)) != NULL) {
          struct stat st;
          memset(&st, 0, sizeof(st));
          st.st_ino = de->d_ino;
          st.st_mode = de->d_type << 12;
          if (filler(buf, de->d_name, &st, 0))
               break;
     }

     closedir(dp);
     return 0;
}

static int xmp_mknod(const char *path, mode_t mode, dev_t rdev)
{
     int res;

     /* On Linux this could just be 'mknod(path, mode, rdev)' but this
        is more portable */
     if (S_ISREG(mode)) {
          res = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
          if (res >= 0)
               res = close(res);
     } else if (S_ISFIFO(mode))
          res = mkfifo(path, mode);
     else
          res = mknod(path, mode, rdev);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_mkdir(const char *path, mode_t mode)
{
     int res;

     res = mkdir(path, mode);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_unlink(const char *path)
{
     int res;

     res = unlink(path);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_rmdir(const char *path)
{
     int res;

     res = rmdir(path);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_symlink(const char *from, const char *to)
{
     int res;

     res = symlink(from, to);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_rename(const char *from, const char *to)
{
     int res;

     res = rename(from, to);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_link(const char *from, const char *to)
{
     int res;

     res = link(from, to);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_chmod(const char *path, mode_t mode)
{
     int res;

     res = chmod(path, mode);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_chown(const char *path, uid_t uid, gid_t gid)
{
     int res;

     res = lchown(path, uid, gid);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_truncate(const char *path, off_t size)
{
     int res;

     res = truncate(path, size);
     if (res == -1)
          return -errno;

     return 0;
}

#ifdef HAVE_UTIMENSAT
static int xmp_utimens(const char *path, const struct timespec ts[2])
{
     int res;

     /* don't use utime/utimes since they follow symlinks */
     res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
     if (res == -1)
          return -errno;

     return 0;
}
#endif

static int xmp_create(const char *path, mode_t mode,
                      struct fuse_file_info *fi)
{
     int res;

     res = open(path, fi->flags, mode);
     if (res == -1)
          return -errno;

     fi->fh = res;
     return 0;
}

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
     int res;

     res = open(path, fi->flags);
     if (res == -1)
          return -errno;

     fi->fh = res;
     return 0;
}


/*
 * now in main... infp will be the stdin (in file descriptor)
 * and outfp will be the stdout (out file descriptor)
 * have fun
 */

/* int main(int argc, char **argv) */
/* { */
/*      int infp, outfp; */
/*      char buf[128]; */

/*      if (popen2("your-program-B", &infp, &outfp) <= 0) */
/*      { */
/*           printf("Unable to exec your-program-B\n"); */
/*           exit(1); */
/*      } */

/*      memset (buf, 0x0, sizeof(buf)); */

/*      write(infp, "Z\n", 2); */
/*      write(infp, "D\n", 2); */
/*      write(infp, "A\n", 2); */
/*      write(infp, "C\n", 2); */
/*      close(infp); */
/*      read(outfp, buf, 128); */
/*      printf("buf = '%s'\n", buf); */
/*      return 0; */
/* } */

static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
     StringBuilder *sb = sb_create();

     int fd;
     int res;

     int outfp;
     /* char buf[128]; */

     if (popen2(path, NULL, &outfp) <= 0)
     {
          return -errno;
     }
     /* memset (buf, 0x0, sizeof(buf)); */

     /* if(fi == NULL) */
     /* 	fd = open(path, O_RDONLY); */
     /* else */
     fd = fi->fh;

     /* if (fd == -1) */
     /* 	return -errno; */

     read(outfp, buf, 128);

     pf(buf);
     res=128;

     // res = pread(fd, buf, size, offset);
     //if (res == -1)
     //	res = -errno;

     // close the process here
     /* if(fi == NULL) */
     /* 	close(fd); */
     return res;
}

static int xmp_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
     int fd;
     int res;

     (void) fi;
     if(fi == NULL)
          fd = open(path, O_WRONLY);
     else
          fd = fi->fh;

     if (fd == -1)
          return -errno;

     res = pwrite(fd, buf, size, offset);
     if (res == -1)
          res = -errno;

     if(fi == NULL)
          close(fd);
     return res;
}

static int xmp_statfs(const char *path, struct statvfs *stbuf)
{
     int res;

     res = statvfs(path, stbuf);
     if (res == -1)
          return -errno;

     return 0;
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{
     (void) path;
     close(fi->fh);
     return 0;
}

static int xmp_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
     /* Just a stub.	 This method is optional and can safely be left
        unimplemented */

     (void) path;
     (void) isdatasync;
     (void) fi;
     return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int xmp_fallocate(const char *path, int mode,
                         off_t offset, off_t length, struct fuse_file_info *fi)
{
     int fd;
     int res;

     (void) fi;

     if (mode)
          return -EOPNOTSUPP;

     if(fi == NULL)
          fd = open(path, O_WRONLY);
     else
          fd = fi->fh;

     if (fd == -1)
          return -errno;

     res = -posix_fallocate(fd, offset, length);

     if(fi == NULL)
          close(fd);
     return res;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int xmp_setxattr(const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
     int res = lsetxattr(path, name, value, size, flags);
     if (res == -1)
          return -errno;
     return 0;
}

static int xmp_getxattr(const char *path, const char *name, char *value,
                        size_t size)
{
     int res = lgetxattr(path, name, value, size);
     if (res == -1)
          return -errno;
     return res;
}

static int xmp_listxattr(const char *path, char *list, size_t size)
{
     int res = llistxattr(path, list, size);
     if (res == -1)
          return -errno;
     return res;
}

static int xmp_removexattr(const char *path, const char *name)
{
     int res = lremovexattr(path, name);
     if (res == -1)
          return -errno;
     return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations xmp_oper = {
     .init           = xmp_init,
     .getattr	= xmp_getattr,
     .access		= xmp_access,
     .readlink	= xmp_readlink,
     .readdir	= xmp_readdir,
     .mknod		= xmp_mknod,
     .mkdir		= xmp_mkdir,
     .symlink	= xmp_symlink,
     .unlink		= xmp_unlink,
     .rmdir		= xmp_rmdir,
     .rename		= xmp_rename,
     .link		= xmp_link,
     .chmod		= xmp_chmod,
     .chown		= xmp_chown,
     .truncate	= xmp_truncate,
#ifdef HAVE_UTIMENSAT
     .utimens	= xmp_utimens,
#endif
     .open		= xmp_open,
     .create 	= xmp_create,
     .read		= xmp_read,
     .write		= xmp_write,
     .statfs		= xmp_statfs,
     .release	= xmp_release,
     .fsync		= xmp_fsync,
#ifdef HAVE_POSIX_FALLOCATE
     .fallocate	= xmp_fallocate,
#endif
#ifdef HAVE_SETXATTR
     .setxattr	= xmp_setxattr,
     .getxattr	= xmp_getxattr,
     .listxattr	= xmp_listxattr,
     .removexattr	= xmp_removexattr,
#endif
};

int main(int argc, char *argv[])
{
     umask(0);
     return fuse_main(argc, argv, &xmp_oper, NULL);
}
