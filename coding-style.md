
# K2 Coding Style
## Short demo of the concepts below
``` c++
//<!--
//    (C)opyright Futurewei Technologies, inc, 2019
//-->

#include <string>
#include <vector>

// to build an RPC server
#include "transport/RPCDispatcher.h"
// to handle incoming tcp traffic
#include "transport/TCPListener.h"

namespace service {

// This class describes the Master RPC service.
// It implements the RPC handlers
class MyClass {
private:
    // our RPC dispatcher
    shared_ptr<RPCDispatcher> _dispatcher;

public:
    MyClass(shared_ptr<RPCDispatcher> dispatcher):
        _dispatcher(dispatcher) {
    }

}; // class MyClass

} // namespace service
```

## Copyright and Licence block
``` c++
//<!--
//    (C)opyright Futurewei Technologies, inc, 2019
//-->
```

## Folders/Modules
- All source files go in the `src` folder, in a subfolder for the module to which they belong
- Avoid dumping grounds such as the `common` or `util` folders as much as possible
- folder names are all in `lowercase` and are as short as possible(i.e. one word) but representative (e.g. node, client, etc.). If you have to use multiple words in the name, use underscore to separate (`"round_table"`)
- Provide a `README.md` file in your module
- Create CMakeLists.txt for your module and integrate with the rest of the project by adding yourself to your parent folder

## Files
- Files are named using `PascalCase`
    - If name contains several words, each of them should start with capital letter
        - Examples:
            - `PartitionManager.h`
            - `NodePoolMain.cpp`
- Header files have the `.h` extension, source files use the `.cpp` extension.
- All files must have a license and copyright blurb. Use `#pragma once` instead of an include guard.
- Header files which contain a public part of the interface of Seastar go in the `include` directory.

## Comments
- Use only single-line comments (`//`)
- Each core component class or interface description must start with comments, describing its relationship to other components
- All functions of all interfaces must be preceded with comments

## Variable naming
- minimum 3 letters
- use single-letter variables only inside for loops (basically only `i` is allowed as it means `index`), or inside very short lambdas.

## Templates
- Use judiciously
- single-letter type names are allowed but must be upper case (e.g. `T`, `K`, `V`)

## Classes and structures
- Named using `PascalCase`
    - prefix private members with underscore, e.g. `void _processPacket(Packet& pkt)`
- Fully describe class meaning
- Abstract class representing interface starts with ‘I’ letter
    - Examples
        - `class MessageInitiatedTaskRequest`
        - `class IModule`
- Class members (variables, functions), local variables:
    - Named using `camelCase` - starts with lower case letter

## Whitespace
- Use 4 spaces only; NEVER tabs.

## Including header files
- In any file, to include a public header file (one in the `include` directory), use an absolute path with `<>` like this:
    ```c++
    #include <k2/transport/Listener.h>
    ```
- In any private file, to include a private header file (one in the `src` directory), use an absolute path with `""` like this:
    ```c++
    #include "transport/Listener.h"
    ```

## Braced blocks

- [Preferred] All nested scopes are braced, even when the language allows omitting the braces (such as an if-statement), this makes patches simpler and is more consistent.
- [Preferred] Class and function brace should start with new line. Within the function, brace location is flexible. Body is indented.
    ```c++
    void Function() 
    {
        if (some condition) {
            stmt;
        } else {
            stmt;
        }
    }
    ```
- An exception is namespaces -- the body is _not_ indented, to prevent files that are almost 100% whitespace left margin.

## Function parameters

- [Preferred] Avoid output parameters; use return values instead.  In/out parameters are tricky, but in some cases they are relatively standard, such as serialization/deserialization.
- If a function accepts a lambda or an `std::function`, make it the last argument, so that it can be easily provided inline:
    ```c++
    template <typename Func>
    void FunctionAcceptingLambda(int a, int b, Func func);

    int blah() {
        return FunctionAcceptingLambda(2, 3, [] (int x, int y) {
            return x + y;
        });
    }
    ```

## Complex return types
- [Preferred] If a function returns a complicated return type, put its return type on a separate line, otherwise it becomes hard to see where the return type ends and where the function name begins:

    ```c++
    template <typename T1, T2>
    template <typename T3, T4>
    std::vector<typename ClassA<T1, T2>::some_nested_class<T3, T4>>  // I'm the return type
    ClassA<T1, T2>::a_function(T3 a, T4 b) {                         // And I'm the function name
        // ...
    }
    ```

## Whitespace around operators
- Whitespace around operators should match their precedence: high precedence = no spaces, low precedency = add spaces:
    ```c++
        return *a + *b;  // good
        return * a+* b;  // bad
    ```

- `if`, `while`, `return` (and `template`) are not function calls, so they get a space after the keyword.

## Long lines

If a line becomes excessively long (>160 characters), or is just complicated, break it into two or more lines.  The second (and succeeding lines) are _continuation lines_, and have a double indent:

    ```c++
        if ((some_condition && some_other_condition)
                || (more complicated stuff here...)   // continuation line, double indent
                || (even more complicated stuff)) {   // another continuation line
            do_something();  // back to single indent
        }
    ```

Of course, long lines or complex conditions may indicate that refactoring is in order.

## Line Ending Character
Use \n(lf)



