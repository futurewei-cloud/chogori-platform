/*
MIT License

Copyright(c) 2020 Futurewei Cloud

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

#pragma once
#include <assert.h>
#include <cstddef>

namespace k2
{

#define K2_LINKED_LIST_NODE_VARIABLE ___K2_IntrusiveLinkedList_Node__


class IntrusiveLinkedListNode
{
    template<typename ALL> friend class IntrusiveLinkedList;
protected:
    void* prev = nullptr;
    void* next = nullptr;

    bool isEmpty() { return prev == nullptr && next == nullptr; }
    void clear()
    {
        prev = nullptr;
        next = nullptr;
    }
};

//
//  Boost is known for making simple things complicated to extent of complete useless.
//  boost::intrusive::list is one of such examples. I used it before got tired of Googling about how to do basic operations.
//  I created mine bicycle, but at least i understand everything about what's doing and code is small enough to find an answers.
//  Additional feature: iteration here supports deletion of iterated element.
//
template<typename T>
class IntrusiveLinkedList
{
public:
    class Node
    {
        template<typename ALL> friend class IntrusiveLinkedList;
    protected:
        void* prev = nullptr;
        void* next = nullptr;

        bool isEmpty() const { return prev == nullptr && next == nullptr; }
        void clear()
        {
            prev = nullptr;
            next = nullptr;
        }
    };

protected:
    T* head = nullptr;
    T* tail = nullptr;
    size_t count = 0;

    static IntrusiveLinkedListNode& node(T& element) { return element.K2_LINKED_LIST_NODE_VARIABLE; }
    static T*& prev(T& element) { return (T*&)node(element).prev; }
    static T*& next(T& element) { return (T*&)node(element).next; }

    static T*& prev(T* element) { return prev(*element); }
    static T*& next(T* element) { return next(*element); }

    static bool notLinked(T& element) { return node(element).isEmpty(); }

public:

    size_t size() const { return count; }

    bool isEmpty() const { return size() == 0; }

    void remove(T& element)
    {
        if(prev(element))
            next(prev(element)) = next(element);
        else
            head = next(element);

        if(next(element))
            prev(next(element)) = prev(element);
        else
            tail = prev(element);

        node(element).clear();
        count--;
    }

    void addAfter(T& position, T& element)
    {
        assert(notLinked(element));

        prev(element) = &position;
        next(element) = next(position);

        next(position) = &element;

        if(tail == &position)
            tail = &element;

        count++;
    }

    void addBefore(T& position, T& element)
    {
        assert(notLinked(element));

        prev(element) = prev(position);
        next(element) = &position;

        prev(position) = &element;

        if(head == &position)
            head = &element;

        count++;
    }

    void pushBack(T& element)
    {
        if(tail)
            addAfter(*tail, element);
        else
        {
            assert(notLinked(element));
            assert(head == nullptr);
            head = tail = &element;
            count++;
        }
    }

    void pushFront(T& element)
    {
        if(head)
            addBefore(*head, element);
        else
        {
            assert(notLinked(element));
            assert(tail == nullptr);
            head = tail = &element;
            count++;
        }
    }

    class Iterator
    {
    protected:
        T* current;
        T* next;

        void setCurrent(T* node)
        {
            current = node;
            next = current ? IntrusiveLinkedList::next(current) : nullptr;
        }

    public:
        Iterator(const Iterator&) = default;
        Iterator& operator=(const Iterator&) = default;
        Iterator(T* start) { setCurrent(start); }

        Iterator& operator++() { setCurrent(next); return *this; }

        T& operator*() { return *current; }
        T* operator->() { return current; }

        bool operator==(const Iterator& other) const { return current == other.current; }
        bool operator!=(const Iterator& other) const { return current != other.current; }
    };

    Iterator begin() const
    {
        return Iterator(head);
    }

    Iterator end() const
    {
        return Iterator(nullptr);
    }
};

#define K2_LINKED_LIST_NODE template<typename T> friend class IntrusiveLinkedList; IntrusiveLinkedListNode K2_LINKED_LIST_NODE_VARIABLE;

}  //  namespace k2
