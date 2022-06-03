// A Trie class modified from:
// https://www.techiedelight.com/cpp-implementation-trie-data-structure/

#include <string>
#include <tuple>
#include <mutex>

#pragma once

// Define the character size
#define CHAR_SIZE 128

// A class to store a Trie node
class Trie
{
public:
    bool isLeaf;
    Trie* character[CHAR_SIZE];
    char value;
    int quantity = 0;

    // Constructor
    Trie()
    {
        this->isLeaf = false;
        this->value = '\0';
        for (int i = 0; i < CHAR_SIZE; i++) {
            this->character[i] = nullptr;
        }
    }

    void insert(std::string);
    bool deletion(Trie*&, std::string);
    int search(std::string);
    bool haveChildren(Trie const*);
    void increment(std::string);
    std::tuple<std::string, int> pop();
};