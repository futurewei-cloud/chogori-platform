'''
MIT License

Copyright(c) 2021 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
'''

def mname_with_vars(mn, i, vars):
    return ''.join([mn, str(i), "(", ','.join(vars), ")"])

def genlst(N):
    print('// _K2_MKLIST(...) generates a list for formatting variables, e.g. _K2_MKLIST(a, b, c) -> "a" = a, "b" = b, "c" = c')
    vrs=["a1"]
    mn="_K2_MKLIST"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0()")
    print("#define", mn + '1(a1) #a1 "=a1"')
    for i in range(2,N):
        vrs.append("a" + str(i))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      '", "',
                      mn + "1(a" +str(i) + ")"])
        print(v)


def genvars(N):
    print('// _K2_MKVARS(...) generates a list for formatting variables, e.g. _K2_MKVARS(a, b, c) -> o.a, o.b, o.c')
    vrs=["a1"]
    mn = "_K2_MKVARS"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0()")
    print("#define", mn + "1(a1) , o.a1")
    for i in range(2,N):
        vrs.append("a" + str(i))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      mn + "1(a" +str(i) + ")"])
        print(v)


def genfromjson(N):
    print('// _K2_FROM_JSON(...) generates from-json entries, e.g. _K2_FROM_JSON(a) -> j.at("a").get_to(o.a);')
    vrs = ["a1"]
    mn = "_K2_FROM_JSON"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0() (void)o; (void)j;")
    print("#define", mn + "1(a1) j.at(#a1).get_to(o.a1);")
    for i in range(2, N):
        vrs.append("a" + str(i))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      mn + "1(a" + str(i) + ")"])
        print(v)

def gentojson(N):
    print(
        '// _K2_TO_JSON(...) generates from-json entries, e.g. _K2_TO_JSON(a1, a2) -> {"a1", o.a1}, {"a2", o.a2}')
    vrs = ["a1"]
    mn = "_K2_TO_JSON"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0()")
    print("#define", mn + "1(a1) {#a1, o.a1}")
    for i in range(2, N):
        vrs.append("a" + str(i))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      ", ",
                      mn + "1(a" + str(i) + ")"])
        print(v)

def gentostring(N):
    print(
        '// _K2_TO_STRING_LIST(...) converts list of args to list of stringed args, e.g. _K2_TO_STRING_LIST(a1, a2) -> "a1", "a2"')
    vrs = ["a1"]
    mn = "_K2_TO_STRING_LIST"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0()")
    print("#define", mn + "1(a1) #a1")
    for i in range(2, N):
        vrs.append("a" + str(i))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      ",",
                      mn + "1(a" + str(i) + ")"])
        print(v)


def gen_enum_if_stmt(N):
    print(
        '''// _K2_ENUM_IF_STMT(Name, ...) generates an if statement for each arg to fit in the enum-generator
// macro, e.g. _K2_ENUM_IF_STMT(NAME, a1, a2) -> if(str=="a1") return NAME::a1; if (str=="a2")...
        ''')
    vrs = ["NAME", "a1"]
    mn = "_K2_ENUM_IF_STMT"
    print("#define", mn + "(...) _K2_OVERLOADED_MACRO(" + mn + ", __VA_ARGS__)")
    print("#define", mn + "0()")      # 0-arg not supported
    print("#define", mn + "1(NAME)")  # 1-arg not supported
    print("#define", mn + "2(NAME,a1) if(str==#a1) return NAME::a1;")
    for i in range(3, N):
        vrs.append("a" + str(i-1))
        v = " ".join(["#define",
                      mname_with_vars(mn, i, vrs),
                      mname_with_vars(mn, i-1, vrs[:-1]),
                      mn + "2(NAME, a" + str(i-1) + ")"])
        print(v)

if __name__ == "__main__":
    print('''/*
MIT License

Copyright(c) 2021 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

/* This entire file has been generated by common/gen_macros.py" */

#pragma once
#include "MacroUtils.h"
    ''')
    N = 60
    genlst(N)
    print()
    genvars(N)
    print()
    genfromjson(N)
    print()
    gentojson(N)
    print()
    gentostring(N)
    print()
    gen_enum_if_stmt(N)
