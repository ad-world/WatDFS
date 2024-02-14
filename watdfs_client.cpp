//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "watdfs_client.h"
#include "debug.h"
#include <iostream>
INIT_LOG

#include "rpc.h"

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
    void *userdata = nullptr;

    // TODO: save `path_to_cache` and `cache_interval` (for A3).

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.

    *ret_code = ret;

    // Return pointer to global state data.
    return userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.
    // TODO: tear down the RPC library by calling `rpcClientDestroy`.
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    DLOG("watdfs_cli_getattr called for '%s'", path);
    
    // getattr has 3 arguments.
    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // The second argument is the stat structure. This argument is an output
    // only argument, and we treat it as a char array. The length of the array
    // is the size of the stat structure, which we can determine with sizeof.
    arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (unsigned int) sizeof(struct stat); // statbuf
    args[1] = (void *)statbuf;

    // The third argument is the return code, an output only argument, which is
    // an integer.
    // TODO: fill in this argument type.

    // The return code is not an array, so we need to hand args[2] an int*.
    // The int* could be the address of an integer located on the stack, or use
    // a heap allocated integer, in which case it should be freed.
    // TODO: Fill in the argument
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    int rpc_ret = 0;
    int func_ret = 0;
    args[2] = (void* )&func_ret;
    // Finally, the last position of the arg types is 0. There is no
    // corresponding arg.
    arg_types[3] = 0;

    // MAKE THE RPC CALL
    rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = func_ret;
    }

    if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then 
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    // Called to create a file.
    DLOG("watdfs_cli_mknod called for '%s'", path);

    // Four arguments: path, mode, dev, retcode
    int ARG_COUNT = 4;

    // Create void pointer array for the arguments
    void **args = new void*[ARG_COUNT];

    // Create int array for argument types
    int arg_types[ARG_COUNT + 1];

    // pathlen + 1 for null terminator
    int pathlen = strlen(path) + 1;

    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Second argument is mode, and mode is an input with type int.
    arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);

    // Set second argument to mode
    args[1] = (void *) &mode;

    // Thid argument is dev, and dev is an input with type long
    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    // Set third argument to dev 
    args[2] = (void* ) &dev;

    // The rpc return value
    int rpc_ret = 0;
    int func_ret = 0;

    // The last argument is the rpc return output value
    arg_types[3] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    // The address of the rpc ret value
    args[3] = (void* )&func_ret;

    arg_types[4] = 0;

    // Setting the rpc_ret value
    rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("mknod rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = func_ret;
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}


int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    // Called during open.
    // You should fill in fi->fh.
    
    // Declaring arg count
    int ARG_COUNT = 3;

    // Arg array
    void **args = new void*[ARG_COUNT];

    // Create int array for argument types
    int arg_types[ARG_COUNT + 1];

    // pathlen + 1 for null terminator
    int pathlen = strlen(path) + 1;

    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Setting second argument to input, output, array, and char
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u) | (unsigned int)sizeof(struct fuse_file_info);

    // Setting first argument to file handler
    args[1] = (void* )fi;

    int rpc_ret = 0;
    int func_ret = 0;
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    args[2] = (void *) &func_ret;

    arg_types[3] = 0;

    rpc_ret = rpcCall((char* )"open", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("open rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = func_ret;
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    // Called during close, but possibly asynchronously.
    // Declaring arg count
    int ARG_COUNT = 3;

    // Arg array
    void **args = new void*[ARG_COUNT];

    // Create int array for argument types
    int arg_types[ARG_COUNT + 1];

    // pathlen + 1 for null terminator
    int pathlen = strlen(path) + 1;

    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Setting second argument to input, output, array, and char
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u) |(unsigned int)sizeof(struct fuse_file_info);

    // Setting first argument to file handler
    args[1] = (void* )fi;

    int rpc_ret = 0;
    int func_ret = 0;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    args[2] = (void *) &func_ret;

    arg_types[3] = 0;

    rpc_ret = rpcCall((char* )"release", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("release rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = func_ret;
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    // Read size amount of data at offset of file into buf.
    size_t remaining_bytes = size;
    size_t max_buffer_size = MAX_ARRAY_LEN;
    off_t off = offset;
    // Remember that size may be greater than the maximum array size of the RPC
    // library.

    int func_ret = 0;
    int ARG_COUNT = 6;
    int arg_types[ARG_COUNT + 1];
    int pathlen = strlen(path) + 1;

    int fxn_ret = 0;
    
    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // Buffer type and size
    arg_types[1] = 
        (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) max_buffer_size;
    // Size type
    arg_types[2] = 
        (1u << ARG_INPUT) | (ARG_LONG << 16u);
    // Offset type
    arg_types[3] = 
        (1u << ARG_INPUT) | (ARG_LONG << 16u);
    // File handler type 
    arg_types[4] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int)sizeof(struct fuse_file_info);
    // Return code type
    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    arg_types[6] = 0;


    while (remaining_bytes > max_buffer_size) {
        void **args = new void*[ARG_COUNT];
        // Path pointer        
        args[0] = (void *)path;
        // Buffer pointer
        args[1] = (void *)buf;
        // Size pointer
        args[2] = (void* )&max_buffer_size;
        // Offset pointer
        args[3] = (void* )&off;
        // File handler pointer
        args[4] = (void* )fi;
        // Return code pointer
        args[5] = (void* )&func_ret;

        int rpc_ret = rpcCall((char* )"read", arg_types, args);
        DLOG("size: '%ld', offset: '%ld'", remaining_bytes, off);

        DLOG("INSIDE WHILE LOOP");

        delete[] args;

        if (rpc_ret < 0) {
            fxn_ret = -EINVAL;
            return fxn_ret;
        }

        if (func_ret < 0) {
            fxn_ret = func_ret;
            return fxn_ret;
        }

        if (func_ret < MAX_ARRAY_LEN) {
            fxn_ret += func_ret;
            return fxn_ret;
        }

        off = off + func_ret;
        buf = buf + func_ret;
        remaining_bytes = remaining_bytes - max_buffer_size;
        fxn_ret = fxn_ret + func_ret;
    }

    max_buffer_size = remaining_bytes;
    // At this point, there still may be some remaining bytes
    // Buffer size may have changed
    arg_types[1] = 
        (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) max_buffer_size;
    void **args = new void*[ARG_COUNT];
    // Path pointer        
    args[0] = (void *)path;
    // Buffer pointer
    args[1] = (void *)buf;
    // Size pointer
    args[2] = (void* )&max_buffer_size;
    // Offset pointer
    args[3] = (void* )&off;
    // File handler pointer
    args[4] = (void* )fi;
    // Return code pointer
    args[5] = (void* )&func_ret;

    int rpc_ret = rpcCall((char* )"read", arg_types, args);

    if (rpc_ret < 0) {
        fxn_ret = -EINVAL;
    } else if (func_ret < 0) {
        fxn_ret = func_ret;
    } else {
        fxn_ret += func_ret;
    }

    delete[] args;

    return fxn_ret;
}
int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    // Write size amount of data at offset of file from buf.
    size_t remaining_bytes = size;
    size_t max_buffer_size = MAX_ARRAY_LEN;
    int func_ret = 0;
    off_t off = offset;
    // Remember that size may be greater than the maximum array size of the RPC
    // library.
    int fxn_ret = 0;
    int ARG_COUNT = 6;
    int arg_types[ARG_COUNT + 1];
    int pathlen = strlen(path) + 1;
    
      // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) max_buffer_size;
    arg_types[2] = 
        (1u << ARG_INPUT) | (ARG_LONG << 16u);
    // Offset type
    arg_types[3] = 
        (1u << ARG_INPUT) | (ARG_LONG << 16u);
    // File handler type 
    arg_types[4] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int)sizeof(struct fuse_file_info);
    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    arg_types[6] = 0;

    while (remaining_bytes > max_buffer_size) {
        void **args = new void*[ARG_COUNT];
        // Path pointer        
        args[0] = (void *)path;
        // Buffer pointer
        args[1] = (void *)buf;
        // Size pointer
        args[2] = (void* )&max_buffer_size;
        // Offset pointer
        args[3] = (void* )&off;
        // File handler pointer
        args[4] = (void* )fi;
        // Return code pointer
        args[5] = (void* )&func_ret;

        int rpc_ret = rpcCall((char* )"write", arg_types, args);

        delete[] args;

        if (rpc_ret < 0) {
            fxn_ret = -EINVAL;
            return fxn_ret;
        } else {
            if (func_ret < 0) {
                return fxn_ret;
            }
        }

        off = off + func_ret;
        buf = buf + func_ret;
        remaining_bytes = remaining_bytes - max_buffer_size;
        fxn_ret = fxn_ret + func_ret;
    }

    max_buffer_size = remaining_bytes;
    // At this point, there still may be some remaining bytes
    // Buffer size may have changed
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) max_buffer_size;
    void **args = new void*[ARG_COUNT];
    // Path pointer        
    args[0] = (void *)path;
    // Buffer pointer
    args[1] = (void *)buf;
    // Size pointer
    args[2] = (void* )&max_buffer_size;
    // Offset pointer
    args[3] = (void* )&off;
    // File handler pointer
    args[4] = (void* )fi;
    // Return code pointer
    args[5] = (void* )&func_ret;

    int rpc_ret = rpcCall((char* )"write", arg_types, args);

    if (rpc_ret < 0) {
        fxn_ret = -EINVAL;
    } else if (func_ret < 0) {
        fxn_ret = func_ret;
    } else {
        fxn_ret += func_ret;
    }
    
    delete[] args;

    return fxn_ret;
}
int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    // Change the file size to newsize.
    int ARG_COUNT = 3;
    int arg_types[ARG_COUNT + 1];
    int func_ret = 0;
    int pathlen = strlen(path) + 1;
    void **args = new void*[ARG_COUNT];
    int fxn_ret;
    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Set type of new size
    arg_types[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[1] = (void*)&newsize;

    // Set type of return
    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    args[2] = &func_ret;
    arg_types[3] = 0;

    int rpc_ret = rpcCall((char* )"truncate", arg_types, args);

    if (rpc_ret < 0) {
        DLOG("truncate rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else {
        fxn_ret = func_ret;
    }

    delete[] args;

    return fxn_ret;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.
    // Declaring arg count
    int ARG_COUNT = 3;

    // Arg array
    void **args = new void*[ARG_COUNT];

    // Create int array for argument types
    int arg_types[ARG_COUNT + 1];

    // pathlen + 1 for null terminator
    int pathlen = strlen(path) + 1;

    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Setting second argument to input, output, array, and char
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u);

    // Setting first argument to file handler
    args[1] = (void* )fi;

    int rpc_ret = 0;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    args[2] = (void *) &rpc_ret;

    arg_types[3] = 0;

    rpc_ret = rpcCall((char* )"fsync", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("fsync rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = rpc_ret;
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    // Called during close, but possibly asynchronously.
    // Declaring arg count
    int ARG_COUNT = 3;

    // Arg array
    void **args = new void*[ARG_COUNT];

    // Create int array for argument types
    int arg_types[ARG_COUNT + 1];

    // pathlen + 1 for null terminator
    int pathlen = strlen(path) + 1;

    // Set type of first argument to input, array, and char
    arg_types[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (unsigned int) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // Setting second argument to input, array, and char
    arg_types[1] = 
        (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u) | (unsigned int)sizeof(struct timespec) * 2;

    // Setting first argument to file handler
    args[1] = (void* )ts;

    int rpc_ret = 0;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    args[2] = (void *) &rpc_ret;

    arg_types[3] = 0;

    rpc_ret = rpcCall((char* )"utimensat", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("utimenstat rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = rpc_ret;
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}