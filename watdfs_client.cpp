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
#include <vector>
#include <cstdarg>
#include <cstring>
#include <cerrno>


struct meta {
  int client_flags;
  int file_handler;
  int server_file_handler;
  time_t tc;
};

struct state {
    time_t interval;
    char *cached_path;
    std::map<std::string, struct meta> open_files;
};

void debug(const char* format, ...) {
    va_list args;
    va_start(args, format);

    // We need to safely determine the size of the formatted string.
    // vsnprintf returns the number of characters (excluding the null terminator) 
    // that would have been written if the buffer was large enough.
    int size = vsnprintf(nullptr, 0, format, args) + 1; // Adding one for the null terminator
    va_end(args);

    if (size <= 0) {
        std::cerr << "Error in formatting the debug log message." << std::endl;
        return;
    }

    // Create a vector with the required size to hold the formatted string.
    std::vector<char> buf(size);

    va_start(args, format);
    vsnprintf(buf.data(), size, format, args);
    va_end(args);

    // Output the formatted string to std::cerr
    std::cerr << buf.data() << std::endl;
}


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
            debug("download - Could not download file - file %s not found on server, with error code %d", path, get_attr);
            delete statbuf;
            delete fi;
            return get_attr;
        }

        debug("download - File %s found on server.", path);

        size_t size = statbuf->st_size;

        int fh = open(full_path, O_RDWR);
        if (fh < 0) {
            // Create the file
            debug("download - Creating file %s on client.", full_path);
            mknod(full_path, statbuf->st_mode, statbuf->st_dev);
            fh = open(full_path, O_RDWR);
        }

        fi->flags = O_RDONLY;

        // TRUNCATE THE FILE AT THE CLIENT
        int trunc_ret = truncate(full_path, (off_t) size);

        if (trunc_ret < 0) {
            debug("download - Could not truncate file %s on client, with error code %d", full_path, trunc_ret);
            func_ret = -errno;
            delete statbuf;
            delete fi;
            return func_ret;
        }

        char *buffer = new char[size];
        fi->flags = O_RDONLY;

        // Open file on server
        int open_ret = RPC::open_rpc((void*) userdata, path, fi);
        if (open_ret < 0) {
            debug("download - Could not open file %s on server.", path);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return open_ret;
        }

        debug("download - File %s opened on server.", path);

        int lock_ret = RPC::lock_rpc(path, RW_READ_LOCK);
        if (lock_ret < 0) {
            debug("download - Could not open file %s on server.", path);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return open_ret;
        }

        debug("download - File %s locked on server in read mode.", path);

        // Read file into local buffer
        int read_ret = RPC::read_rpc((void*) userdata, path, buffer, size, 0, fi);
        if (read_ret < 0) {
            debug("download - Could not read file %s on server, with error code %d", path, read_ret);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return read_ret;
        }

        debug("download - File %s read on server.", path);

        int unlock_ret = RPC::unlock_rpc(path, RW_READ_LOCK);
        if (unlock_ret < 0) {
            debug("download - Could not unlock file %s on server.", path);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return unlock_ret;
        }

        debug("download - File %s unlocked on server.", path);

        // Write buffer into file handler
        int write_ret = pwrite(fh, buffer, size, 0);

        if (write_ret < 0) {
            debug("download - Could not write file %s on client, with error code %d", full_path, write_ret);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return write_ret;
        }

        // Update metadata at the client
        struct timespec ts[2] = { statbuf->st_mtim, statbuf->st_mtim };
        int utime_ret = utimensat(0, full_path, ts, 0);

        if (utime_ret < 0) {
            debug("download - Could not utimensat file %s on client, with error code %d", full_path, utime_ret);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return utime_ret;
        }

        // Download complete, release on server
        int release_ret = RPC::release_rpc((void*) userdata, path, fi);
        if (release_ret < 0) {
            debug("download - Could not release file %s on server, with error code %d", path, release_ret);
            delete[] buffer;
            delete statbuf;
            delete fi;
            return release_ret;
        }

        debug("download - File %s released on server.", path);

        int close_ret = close(fh);
        if (close_ret < 0) {
            debug("download - Could not close file %s on client.", full_path);
        }

        delete[] buffer;
        delete fi;
        delete statbuf;
        return close_ret;
    }

    char *full_path(struct state *userdata, char *path) {
        int short_path_len = strlen(path);
        int dir_len = strlen(userdata->cached_path);
        int full_len = dir_len + short_path_len + 1;

        char *full_path = (char *)malloc(full_len);

        // First fill in the directory.
        strcpy(full_path, userdata->cached_path);
        // Then append the path.
        strcat(full_path, path);
        debug("Full path: %s\n", full_path);

        return full_path;
    }

    int upload_file(char *full_path, char *path, struct state *userdata) {
        int func_ret = 0;

        struct stat *client_buf = new struct stat;
        struct stat *server_buf = new struct stat;
        
        int stat_ret = stat(full_path, client_buf);
        
        if ( stat_ret < 0) {
            debug("upload - Could not stat file %s on client.", full_path);
            return stat_ret;
        }

        struct fuse_file_info *server_file_info = new struct fuse_file_info;


        int get_attr_ret = RPC::get_attr_rpc((void*)userdata, (char*)path, server_buf);
        if (get_attr_ret < 0) {
            debug("upload - File not found on server, will mknod on server");
             int mknod_ret = RPC::mknod_rpc((void*) userdata, path, client_buf->st_mode, client_buf->st_dev);
            
            if (mknod_ret < 0) {
                debug("upload - Could not mknod file %s on server, with error code", path, mknod_ret);
                delete client_buf;
                delete server_buf;
                return mknod_ret;
            }

            server_file_info->flags = O_RDWR;

            int open_ret = RPC::open_rpc((void*)userdata, path, server_file_info);

            if (open_ret < 0) {
                debug("upload - Could not open file %s on server, with error code %d", full_path, open_ret);
                delete client_buf;
                delete server_buf;
                return open_ret;
            }
        } else {
            if (userdata->open_files.find(full_path) == userdata->open_files.end()) {
                debug("upload - FILE IS NOT OPEN AND IT SHOULD BE");
                delete client_buf;
                delete server_buf;
                return -2;
            }

            server_file_info->fh = userdata->open_files[full_path].server_file_handler;
        }
        
        int client_open_ret = open(full_path, O_RDONLY);
        
        if (client_open_ret < 0) {
            debug("upload - Could not open file %s on client.", full_path);
            delete client_buf;
            delete server_buf;
            return -errno;
        }

        char *buffer = new char[client_buf->st_size];
        int read_ret = pread(client_open_ret, buffer, client_buf->st_size, 0);

        if(read_ret < 0) {
            debug("upload - Could not read file %s on client.", full_path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return -errno;
        }

        int lock_ret = RPC::lock_rpc(path, RW_WRITE_LOCK);
        if (lock_ret < 0) {
            debug("upload - Could not lock file %s on server.", path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return lock_ret;
        }

        int trunc_ret = RPC::truncate_rpc((void* )userdata, path, (off_t)client_buf->st_size);
        if (trunc_ret < 0) {
            debug("upload - Could not truncate file %s on server.", path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return trunc_ret;
        }

        int write_ret = RPC::write_rpc((void*)userdata, path, buffer, (off_t)client_buf->st_size, 0, server_file_info);
        if (write_ret < 0) {
            debug("upload - Could not write file %s on server.", path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return write_ret;
        }

        struct timespec ts[2];
        ts[0] = (struct timespec)(client_buf->st_mtim);
        ts[1] = (struct timespec)(client_buf->st_mtim);

        int utim_ret = RPC::utimensat_rpc((void*) userdata, path, ts);

        if (utim_ret < 0) {
            debug("upload - Could not utimensat file %s on server.", path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return utim_ret;
        }

        int unlock_ret = RPC::unlock_rpc(path, RW_WRITE_LOCK);
        if (unlock_ret < 0) {
            debug("upload - Could not unlock file %s on server.", path);
            delete client_buf;
            delete server_buf;
            delete[] buffer;
            return utim_ret;
        }

        int close_ret = close(client_open_ret);

        delete[] buffer;
        delete client_buf;
        delete server_buf;

        return func_ret;
    }

    bool use_file_from_cache(char *full_path, char* path, struct state *userdata) {
        struct stat *client = new struct stat;
        int client_getattr = stat(full_path, client);

        time_t T_c = client->st_mtime;

        delete client;

        std::string file_string = std::string(full_path);
        struct meta file_metadata = userdata->open_files[file_string];
        time_t time_interval = userdata->interval;

        if ((time(0) - file_metadata.tc) < time_interval) {
            userdata->open_files[file_string].tc = time(0);
            return true;
        }

        struct stat *server = new struct stat;

        int server_getattr = RPC::get_attr_rpc((void* )userdata, path, server);
        if(server_getattr < 0) {
            debug("freshness - Could not get_attr file %s on server.", path);
            delete server;
            return false;
        }

        time_t T_s = server->st_mtime;
        delete server;

        // If time is the same, can use cached copy
        if (T_s == T_c) {
            userdata->open_files[file_string].tc = time(0);
            return true;
        }
       
        return false;
    }
}


// SETUP AND TEARDOWN
void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
    // TODO: set up the RPC library by calling `rpcClientInit`.
    int ret = rpcClientInit();
    // TODO: check the return code of the `rpcClientInit` call
    // `rpcClientInit` may fail, for example, if an incorrect port was exported.
    debug("watdfs_cli_init called");
    if (ret != 0) {
        debug("Failed to initialize RPC Client");
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
    struct state *state_ref = (struct state*) userdata;
    char *full_path = helpers::full_path(state_ref, (char *)path);
    std::string path_string = std::string(full_path);
    debug("Calling getattr on %s", full_path);

    debug("Getattr full path: %s", full_path);

    // File is not open
    if (state_ref->open_files.find(path_string) == state_ref->open_files.end()) {
        int download_ret = helpers::download_file((char* )path, full_path, state_ref);
        if (download_ret < 0) {
            debug("getattr - Could not download file %s, with error code %d", path, download_ret);
            delete[] full_path;
            return download_ret;
        }

        int open_ret = open(full_path, O_RDONLY);
        if (open_ret < 0) {
            debug("getattr - Could not open file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }

        int stat_ret = stat(full_path, statbuf);
        if (stat_ret < 0) {
            debug("getattr - Could not stat file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }

        int close_ret = close(open_ret);
        if (close_ret < 0) {
            debug("getattr - Could not close file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }   
    // File is open
    } else {
        int access_mode = state_ref->open_files[path_string].client_flags & O_ACCMODE;
        bool use_from_cache = helpers::use_file_from_cache(full_path, (char* )path, state_ref);
        
        if (!use_from_cache && access_mode == O_RDONLY) {
            int download_ret = helpers::download_file((char* )path, full_path, state_ref);
            if (download_ret < 0) {
                debug("getattr - Could not download file %s, with error code %d", path, download_ret);
                delete[] full_path;
                return download_ret;
            }
            state_ref->open_files[path_string].tc = time(0);
        };

        int stat_ret = stat(full_path, statbuf);
        if (stat_ret < 0) {
            debug("getattr - Could not stat file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
    }

    delete[] full_path;
    return 0;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    struct state *state_ref = (struct state*) userdata;
    char *full_path = helpers::full_path(state_ref, (char *)path);
    std::string path_string = std::string(full_path);
    debug("Calling mknod on %s", full_path);

    // File has not been opened yet
    if (state_ref->open_files.find(path_string) == state_ref->open_files.end()) {
        int mknod_ret = mknod(full_path, mode, dev);
        if (mknod_ret < 0) {
            debug("path: %s",path);
            debug("full path: %s", full_path);
            debug("mknod (file not open) - Could not mknod file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }

        int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
        
        if (upload_ret < 0) {
            debug("mknod - Could not upload file %s, with error code %d", full_path, upload_ret);
        }

        delete [] full_path;
        return upload_ret;
    } else {
    // File is open
        int access_mode = state_ref->open_files[path_string].client_flags & O_ACCMODE;
        if (access_mode == O_RDONLY) return -EMFILE;

        int mknod_ret = mknod(full_path, mode, dev);
        if (mknod_ret < 0) {
            debug("mknod (file already open) - Could not mknod file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }

        bool is_fresh = helpers::use_file_from_cache(full_path, (char* )path, state_ref);
        if (!is_fresh) {
            int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
            if (upload_ret < 0) {
                debug("mknod - Could not upload file %s, with error code %d", full_path, upload_ret);
            }
            state_ref->open_files[path_string].tc = time(0);
            delete[] full_path;
            return upload_ret;
        }

        return 0;
    }
}


int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    char *full_path = helpers::full_path((struct state*) userdata, (char *)path);

    struct state *state_ref = (struct state*) userdata;
    std::string path_string = std::string(full_path);
    debug("Calling open on %s", full_path);

    // If file is open, return error
    if (state_ref->open_files.find(path_string) != state_ref->open_files.end()) {
        debug("open - Trying to open %s which is already open.", full_path);
        delete[] full_path;
        return -EMFILE;
    }

    struct stat *buf = new struct stat;

    int get_attr_ret = RPC::get_attr_rpc(userdata, path, buf);
    
    int func_ret = 0;

    if (get_attr_ret < 0) {
        if (fi->flags != O_CREAT) {
            debug("open - Could not get_attr file %s on server, with error code %d. File does not exist, and was trying to be access without O_CREAT flag", path, get_attr_ret);
            delete[] full_path;
            delete buf;
            return get_attr_ret;
        }

        int open_ret = RPC::open_rpc(userdata, path, fi);
        if (open_ret < 0) {
            debug("open - Could not open file %s on server, with error code %d", path, open_ret);
            delete[] full_path;
            delete buf;
            return open_ret;
        } else {
            int download_ret = helpers::download_file((char* )path, full_path, state_ref);
            if (download_ret < 0) {
                debug("open - Could not download file %s, with error code %d", path, download_ret);
                delete[] full_path;
                delete buf;
                return download_ret;
            }
        }
    } else {
        int download_ret = helpers::download_file((char* )path, full_path, state_ref);
        if (download_ret < 0) {
            debug("open - Could not download file %s, with error code %d", path, download_ret);
            delete[] full_path;
            delete buf;
            return download_ret;
        }

        int open_ret = RPC::open_rpc(userdata, path, fi);
        if (open_ret < 0) {
            debug("open - Could not open file %s on server, with error code %d", path, open_ret);
            delete[] full_path;
            delete buf;
            return open_ret;
        }
    }

    int open_ret = open(full_path, fi->flags);

    if (open_ret < 0) {
        debug("open - Could not open file %s, with error code %d", full_path, -errno);
        delete[] full_path;
        delete buf;
        return -errno;
    }

    debug("server file handle: %d", fi->fh);
    debug("client file handle: %d", open_ret);

    struct meta file = { fi->flags, open_ret, fi->fh, time(0) }; 
    ((struct state*) userdata)->open_files[std::string(full_path)] = file;

    delete[] full_path;
    delete buf;
    return 0;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    // Called during close, but possibly asynchronously.
    struct state* cast_state = (struct state*)userdata;
    char *full_path = helpers::full_path(cast_state, (char*) path);
    int file_access_mode = cast_state->open_files[std::string(full_path)].client_flags;
    int func_ret = 0;

    debug("Calling release on %s", full_path);

    // int release_ret = RPC::release_rpc(userdata, path, fi);
    // if (release_ret < 0) {
    //     debug("release - could not release file on server.")
    // }


    if ((file_access_mode & O_ACCMODE) != O_RDONLY) {
        int push_ret = helpers::upload_file(full_path, (char*)path, cast_state);
        if (push_ret < 0) {
            debug("release - Uploading file failed with: '%d'", push_ret);
            delete[] full_path;
            return push_ret;
        }
    }

    int close_ret = close(cast_state->open_files[std::string(full_path)].file_handler);
    if (close_ret < 0) {
        debug("release - Closing file failed with: '%d'", close_ret);
        delete[] full_path;
        return -errno;
    }  else {
        debug("release - File %s closed, and will be erased from user state.", path);
        cast_state->open_files.erase(std::string(full_path));
    }

    delete[] full_path;

    return func_ret;
}

int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    char *full_path = helpers::full_path((struct state*)userdata, (char* )path);
    std::string file_string = std::string(full_path);
    struct state *cast_state = (struct state*) userdata;
    debug("Calling read on %s", full_path);


    if(cast_state->open_files.find(file_string) == cast_state->open_files.end()) {
        // File isn't open
        debug("read - File %s not open.", full_path);
        delete[] full_path;
        return -EMFILE;
    }

    bool use_from_cache = helpers::use_file_from_cache(full_path, (char *) path, cast_state);

    if (!use_from_cache) {
        int download_code = helpers::download_file((char* )path, full_path, cast_state);
        if (download_code < 0) {
            debug("read - Could not download file %s, with error code %d", path, download_code);
            delete[] full_path;
            return -EMFILE;
        }

        cast_state->open_files[file_string].tc = time(0);
    }

    delete[] full_path;

    return pread(cast_state->open_files[file_string].file_handler, buf, size, offset);
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    struct state *cast_state = (struct state*)userdata;
    char *full_path = helpers::full_path(cast_state, (char*) path);
    std::string file_string = std::string(full_path);
    debug("Calling write on %s", full_path);


    if(cast_state->open_files.find(file_string) == cast_state->open_files.end()) {
        debug("write - File %s not open.", full_path);
        delete[] full_path;
        return -EMFILE;
    }

    int write_code = pwrite(cast_state->open_files[file_string].file_handler, buf, size, offset);

    if (write_code >= 0) {
        bool use_from_cache = helpers::use_file_from_cache(full_path, (char* )path, cast_state);
        if (!use_from_cache) {
            int upload_ret = helpers::upload_file(full_path, (char* ) path, cast_state);
            if (upload_ret < 0) {
                debug("write - Could not upload file %s, with error code %d", full_path, upload_ret);
                delete[] full_path;
                return upload_ret;
            }

            cast_state->open_files[file_string].tc = time(0);
        }
    } else {
        debug("write - Could not write to file %s, with error code %d", full_path, -errno);
        delete[] full_path;
        return -errno;
    } 

    delete[] full_path;
    
    return write_code;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    struct state *state_ref = (struct state*) userdata;
    char *full_path = helpers::full_path(state_ref, (char *)path);
    std::string path_string = std::string(full_path);
    debug("Calling truncate on %s", full_path);

    // File has not been opened yet
    if (state_ref->open_files.find(path_string) == state_ref->open_files.end()) {
        // Download file
        int download_ret = helpers::download_file((char* )path, full_path, state_ref);
        if (download_ret < 0) {
            debug("truncate - Could not download file %s, with error code %d", path, download_ret);
            delete[] full_path;
            return download_ret;
        }
        // Open file on client
        int open_ret = open(full_path, O_RDWR); 
        if (open_ret < 0) {
            debug("truncate - Could not open file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
        // Truncate file on client
        int truncate_ret = truncate(full_path, newsize);
        if(truncate_ret < 0) {
            debug("truncate - Could not truncate file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
        // Upload file to server
        int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
        if (upload_ret < 0) {
            debug("truncate - Could not upload file %s, with error code %d", full_path, upload_ret);
            delete[] full_path;
            return upload_ret;
        }
        // Close file on client
        int close_ret = close(open_ret);
        if (close_ret < 0) {
            debug("truncate - Could not close file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
    } else {
    // File is open
        int access_mode = state_ref->open_files[path_string].client_flags & O_ACCMODE;
        if (access_mode == O_RDONLY) return -EMFILE;
        // Truncate file on client
        int truncate_ret = truncate(full_path, newsize);
        if(truncate_ret < 0) {
            debug("truncate - Could not truncate file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        } 
        // Write freshness check
        bool is_fresh = helpers::use_file_from_cache(full_path, (char* )path, state_ref);
        if (!is_fresh) {
            int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
            if(upload_ret < 0) {
                debug("truncate - Could not upload file %s, with error code %d", full_path, upload_ret);
                delete[] full_path;
                return upload_ret;
            }

            state_ref->open_files[path_string].tc = time(0);
            delete[] full_path;
            return upload_ret;
        }
    }

    delete[] full_path;
    return 0;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.
    struct state *cast_state = (struct state*)userdata;
    char *full_path = helpers::full_path(cast_state, (char*) path);
    std::string file_string = std::string(full_path);

    debug("Calling fsync on %s", full_path);

    if(cast_state->open_files.find(file_string) == cast_state->open_files.end()) {
        debug("fsync - File %s not open.", full_path);
        delete[] full_path;
        return -EMFILE;
    }

    // If file is opened in READ MODE, cannot flush
    if ((cast_state->open_files[file_string].client_flags & O_ACCMODE) == O_RDONLY) {
        debug("fsync - File %s opened in READ MODE.", full_path);
        return -EPERM;
    }

    int push_update = helpers::upload_file(full_path, (char* )path, cast_state); 
    if (push_update < 0) {
        debug("fsync - Could not upload file %s, with error code %d", full_path, push_update);
        delete[] full_path;
        return push_update;
    }

    cast_state->open_files[file_string].tc = time(0);
    delete[] full_path;

    return 0;
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    struct state *state_ref = (struct state*) userdata;
    char *full_path = helpers::full_path(state_ref, (char *)path);
    std::string path_string = std::string(full_path);
    debug("Calling utimensat on %s", full_path);

    // File has not been opened yet
    if (state_ref->open_files.find(path_string) == state_ref->open_files.end()) {
        // Download file
        int download_ret = helpers::download_file((char* )path, full_path, state_ref);
        if (download_ret < 0) {
            debug("utimensat - Could not download file %s, with error code %d", path, download_ret);
            delete[] full_path;
            return download_ret;
        }
        // Open file on client
        int open_ret = open(full_path, O_RDWR); 
        if (open_ret < 0) {
            debug("utimensat - Could not open file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
        // Utimensat file on client
        int utimensat_ret = utimensat(0, full_path, ts, 0);
        if(utimensat_ret < 0) {
            debug("utimensat - Could not utimensat file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
        // Upload file to server
        int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
        if (upload_ret < 0) {
            debug("utimensat - Could not upload file %s, with error code %d", full_path, upload_ret);
            delete[] full_path;
            return upload_ret;
        }
        // Close file on client
        int close_ret = close(open_ret);
        if (close_ret < 0) {
            debug("utimensat - Could not close file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
    } else {
    // File is open
        int access_mode = state_ref->open_files[path_string].client_flags & O_ACCMODE;
        if (access_mode == O_RDONLY) return -EMFILE;
        // Utimensat file on client
        int utimensat_ret = utimensat(0, full_path, ts, 0);
        if(utimensat_ret < 0) {
            debug("utimensat - Could not utimensat file %s, with error code %d", full_path, -errno);
            delete[] full_path;
            return -errno;
        }
        // Write freshness check
        bool is_fresh = helpers::use_file_from_cache(full_path, (char* )path, state_ref);
        if (!is_fresh) {
            int upload_ret = helpers::upload_file(full_path, (char*)path, state_ref);
            if(upload_ret < 0) {
                debug("utimensat - Could not upload file %s, with error code %d", full_path, upload_ret);
                delete[] full_path;
                return upload_ret;
            }
            state_ref->open_files[path_string].tc = time(0);
            delete[] full_path;
            return upload_ret;
        }
    }

    delete[] full_path;
    return 0;
}

