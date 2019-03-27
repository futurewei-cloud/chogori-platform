#pragma once


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

        bool isEmpty() { return prev == nullptr && next == nullptr; }
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

    static IntrusiveLinkedListNode& getNode(T& element) { return element.K2_LINKED_LIST_NODE_VARIABLE; }
    static T*& prev(T& element) { return (T*&)element.K2_LINKED_LIST_NODE_VARIABLE.prev; }
    static T*& next(T& element) { return (T*&)element.K2_LINKED_LIST_NODE_VARIABLE.next; }

    static T*& prev(T* element) { return prev(*element); }
    static T*& next(T* element) { return next(*element); }

    static bool notLinked(T& element) { return getNode(element).isEmpty(); }

public:

    size_t size() { return count; }

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

    Iterator begin()
    {
        return Iterator(head);
    }

    Iterator end()
    {
        return Iterator(nullptr);
    }
};

#define K2_LINKED_LIST_NODE template<typename T> friend class IntrusiveLinkedList; IntrusiveLinkedListNode K2_LINKED_LIST_NODE_VARIABLE;

};  //  namespace k2
