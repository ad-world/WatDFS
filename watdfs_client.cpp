//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "watdfs_client.h"
#include "debug.h"
#include <iostream>
INIT_LOG

#include "rpc.h"
#include <sys/stat.h>
#include <map>
#include "rpc_functions.h"

struct meta {
  int client_mode;
  int server_mode;
  time_t tc;
};

struct state {
    time_t interval;
    char *cached_path;
    std::map<std::string, struct meta> open_files;
};




/* Helper function namespace*/
namespace helpers {
    int download_file(char *path, char *full_path, struct state *userdata) {
        int sys_ret = 0;
        struct fuse_file_info *fi = new struct fuse_file_info;
        struct stat *statbuf = new struct stat;

        int func_ret = 0;

        // Check if file exists on server
        int get_attr = RPC::get_attr_rpc((void* )userdata, path, statbuf);

        if (get_attr < 0) {
            delete statbuf;
            delete fi;
            return get_attr;
        }

        DLOG("File %s found on server.", path);

        size_t size = statbuf->st_size;

        int fh = open(full_path, O_RDWR);
        if (fh < 0) {
            // Create the file
            mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            fh = open(full_path, O_RDWR);
        }

        fi->flags = O_RDONLY;

        // TRUNCATE THE FILE AT THE CLIENT
        int trunc_ret = truncate(full_path, (off_t) size);

        if (trunc_ret < 0) func_ret = -errno;

        char *buffer = new char[size];

        // Open file on server
        int open_ret = RPC::open_rpc((void*) userdata, path, fi);
        if (open_ret < 0) func_ret = open_ret;

        DLOG("File %s opened on server.", path);

        // Read file into local buffer
        int read_ret = RPC::read_rpc((void*) userdata, path, buffer, size, 0, fi);
        if (read_ret < 0) func_ret = read_ret;

        DLOG("File %s read on server.", path);

        // Write buffer into file handler
        int write_ret = pwrite(fh, buffer, size, 0);

        if (write_ret < 0) {
            func_ret = -errno;
            delete[] buffer;
            delete statbuf;
            delete fi;
            return func_ret;
        }

        // Update metadata at the client
        struct timespec ts[2] = { statbuf->st_mtim, statbuf->st_mtim };
        int utime_ret = utimensat(0, full_path, ts, 0);

        // Download complete, release on server
        int release_ret = RPC::release_rpc((void*) userdata, path, fi);
        if (release_ret < 0) func_ret = release_ret;
        DLOG("File %s released on server.", path);

        int close_ret = close(utime_ret);

        delete[] buffer;
        delete fi;
        delete statbuf;
        return func_ret;
    }

    char *full_path(struct state *userdata, char *path) {
        int path_len = strlen(path);
        int dir_len = strlen(userdata->cached_path);

        int length = dir_len + path_len + 1;
        char *full_path = new char[length];
        strncpy(full_path, userdata->cached_path, dir_len);
        strncat(full_path, path, path_len);

        return full_path;
    }

    int upload_file(char *full_path, char *path, struct state *userdata) {
        struct fuse_file_info *fi = new struct fuse_file_info;
        struct stat *buf = new struct stat;
        fi->flags = O_RDWR;
        int func_ret = 0;

        int stat_ret = stat(full_path, buf);

        if ( stat_ret < 0) {
            func_ret = -errno;
        }

        int open_ret = RPC::open_rpc((void*) userdata, path, fi);

        if (open_ret < 0) {
            int mknod_ret = RPC::mknod_rpc((void*) userdata, path, buf->st_mode, buf->st_dev);
            open_ret = RPC::open_rpc((void*)userdata, path, fi);
        }

        if (open_ret < 0) {
            func_ret = -errno;
        }

        open_ret = open(full_path, O_RDONLY);
        fi->flags = O_RDWR;

        char *buffer = new char[buf->st_size];
        int read_ret = pread(open_ret, buffer, buf->st_size, 0);

        if(read_ret < 0) {
            func_ret = -errno;
        }

        int trunc_ret = RPC::truncate_rpc((void* )userdata, path, (off_t)buf->st_size);
        if (trunc_ret < 0) func_ret = trunc_ret;

        int write_ret = RPC::write_rpc((void*)userdata, path, buffer, (off_t)buf->st_size, 0, fi);
        if (write_ret < 0) func_ret = write_ret;

        struct timespec ts[2];
        ts[0] = (struct timespec)(buf->st_mtim);
        ts[1] = (struct timespec)(buf->st_mtim);

        int utim_ret = watdfs_cli_utimensat((void*) userdata, path, ts);
        if (utim_ret < 0) func_ret = utim_ret;
        int get_attr_ret = RPC::get_attr_rpc((void*) userdata, path, buf);
        if (get_attr_ret < 0 ) func_ret = get_attr_ret;

        delete[] buffer;
        delete fi;
        delete buf;

        return func_ret;
    }
}


// SETUP AND TEARDOWN
void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
    // TODO: set up the RPC library by calling `rpcClientInit`.
    int ret = rpcClientInit();
    // TODO: check the return code of the `rpcClientInit` call
    // `rpcClientInit` may fail, for example, if an incorrect port was exported.
    DLOG("watdfs_cli_init called");
    if (ret != 0) {
        std::cerr << "Failed to initialize RPC Client" << std::endl;
    }

    // It may be useful to print to stderr or stdout during debugging.
    // Important: Make sure you turn off logging prior to submission!
    // One useful technique is to use pre-processor flags like:
    // # ifdef PRINT_ERR
    // std::cerr << "Failed to initialize RPC Client" << std::endl;
    // #endif
    // Tip: Try using a macro for the above to minimize the debugging code.

    // TODO Initialize any global state that you require for the assignment and return it.
    // The value that you return here will be passed as userdata in other functions.
    // In A1, you might not need it, so you can return `nullptr`.
    struct state *userdata = new struct state;
    userdata->cached_path = new char[strlen(path_to_cache)];
    userdata->interval = cache_interval;
    strcpy(userdata->cached_path, path_to_cache); 

    // TODO: save `path_to_cache` and `cache_interval` (for A3).

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.

    *ret_code = ret;

    // Return pointer to global state data.
    return (void* )userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.
    // TODO: tear down the RPC library by calling `rpcClientDestroy`.
    rpcClientDestroy();
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
   return RPC::get_attr_rpc(userdata, path, statbuf);
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    return RPC::mknod_rpc(userdata, path, mode, dev);
}


int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    char *full_path = helpers::full_path((struct state*) userdata, (char *)path);

    struct state *state_ref = (struct state*) userdata;
    std::string path_string = std::string(full_path);

    // If file is open, return error
    if (state_ref->open_files.find(path_string) != state_ref->open_files.end()) {
        DLOG("Trying to open %s which is already open.", full_path);
        delete[] full_path;
        return -EMFILE;
    }

    struct stat *buf = new struct stat;

    int get_attr_ret = RPC::get_attr_rpc(userdata, path, buf);
    
    int func_ret = 0;

    if (get_attr_ret < 0) {
        // Do something, file doesn't exist on the server
    } else {
        int download_ret = helpers::download_file(full_path, (char* )path, (state* ) userdata);
        if (download_ret < 0) func_ret = download_ret;
    }

    if (func_ret >= 0) {
        int open_ret = open(full_path, fi->flags);

        if (open_ret < 0) {
            delete[] full_path;
            delete buf;
            return -errno;
        }

        struct meta file = { fi->flags, open_ret, time(0) }; 
        ((struct state*) userdata)->open_files[std::string(full_path)] = file;
        func_ret = 0;
    } else {
        func_ret = -errno;
    }

    delete[] full_path;
    delete buf;
    return func_ret;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    // Called during close, but possibly asynchronously.
    struct state* cast_state = (struct state*)userdata;
    char *full_path = helpers::full_path(cast_state, (char*) path);
    int file_access_mode = cast_state->open_files[std::string(full_path)].client_mode;
    int func_ret = 0;

    if ((file_access_mode & O_ACCMODE) != O_RDONLY) {
        int push_ret = helpers::upload_file(full_path, (char*)path, cast_state);
        if (push_ret < 0) return push_ret;
    }

    int close_ret = close(cast_state->open_files[std::string(full_path)].server_mode);
    if (close_ret < 0) {
        func_ret = -errno;
    }  else {
        cast_state->open_files.erase(std::string(full_path));
    }

    delete[] full_path;

    return func_ret;
}

int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
   return RPC::read_rpc(userdata, path, buf, size, offset, fi);
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    return RPC::write_rpc(userdata, path, buf, size, offset, fi);
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    return RPC::truncate_rpc(userdata, path, newsize);
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.
    return RPC::fsync_rpc(userdata, path, fi);
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    // Called during close, but possibly asynchronously.
    return RPC::utimensat_rpc(userdata, path, ts);
}

