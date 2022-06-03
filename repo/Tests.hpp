#include "Workflow.hpp"
#include "FileManager.hpp"
#include "Mapper.hpp"

#pragma once

class Test {
	template <class T> static bool test(T actual, T expected, std::string testName);
public:
	static bool runTests();
};