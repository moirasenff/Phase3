#include "Trie.hpp"

#pragma once

class Reducer {
	std::string mOutDir;
public:
	// constructor takes in the out path
	Reducer(std::string);
	// reduces the trie into a word and key (quantity) and calls export
	bool reduce(Trie*);
};