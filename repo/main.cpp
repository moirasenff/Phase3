#include "FileManager.hpp"
#include "Workflow.hpp"
#include "Tests.hpp"

int main(int argc, char* argv[]) {

	BOOST_LOG_TRIVIAL(debug) << "Running unit tests...";
	if (Test::runTests()) {
		BOOST_LOG_TRIVIAL(debug) << "All unit tests passed!";
	}
	else {
		BOOST_LOG_TRIVIAL(warning) << "One or more unit tests failed!";
	}

	if (argc != 4) {
		BOOST_LOG_TRIVIAL(error) << "Malformed arguments! Please run <executable> <input dir> <temp dir> <output dir>";
		return -1;
	}
	std::string input = argv[1];
	std::string temp = argv[2];
	std::string output = argv[3];
	if (!FileManager::init(input, temp, output)) {
		BOOST_LOG_TRIVIAL(error) << "Filesystem init failed to complete!";
		return -1;
	}
	Workflow wf;
	// if checks pass, dirs are valid, pass them off to the workflow
	if (!wf.run(input, temp, output)) {
		BOOST_LOG_TRIVIAL(error) << "Workflow failed to complete!";
		FileManager::printFinal(false, output);
		return -1;
	}
	else {
		BOOST_LOG_TRIVIAL(info) << "Workflow completed successfully!";
		FileManager::printFinal(true, output);
	}
}