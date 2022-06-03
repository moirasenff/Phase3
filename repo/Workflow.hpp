#include <string>
#include <iostream>
#include <boost/log/trivial.hpp>
#include <stdlib.h>
#include <stdio.h>
#include <string>

#include "Trie.hpp"

#pragma once

class Workflow {
	// params: input unsorted string vector
	// returns: "sorted" trie of words
	void sort(Trie*, std::vector<std::string>);
public:
	// params: input dir, temp dir, output dir
	// returns: pass/fail bool
	bool run(std::string, std::string, std::string);
	static bool test();
};