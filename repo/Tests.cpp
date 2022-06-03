#include "Tests.hpp"


template <class T> bool Test::test(T actual, T expected, std::string testName) {
	if (expected == actual) {
		BOOST_LOG_TRIVIAL(debug) << "UNIT TEST: " << testName << " PASSED!";
		return true;
	}
	else {
		BOOST_LOG_TRIVIAL(warning) << "UNIT TEST: " << testName << " FAILED!";
		BOOST_LOG_TRIVIAL(warning) << "Expected: " << expected << " Actual: " << actual;
		return false;
	}
}
bool Test::runTests() {
	// list all the tests here
	if (
		test(0, 0, "Test class functional")
		&& test(FileManager::test(), true, "FileManager test") 
		&& test(Mapper::test(), true, "Mapper test")
		&& test(Workflow::test(), true, "Workflow/Reducer/Trie test")
		) {
		return true;
	}
	else {
		return false;
	}
}