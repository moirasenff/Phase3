#include "FileManager.hpp"
#include <sstream>
#include <string>
#include <fstream>
#include <boost/log/trivial.hpp>

std::vector<std::string> FileManager::read(std::string directory, int key) {
	std::vector<std::string> retVect;
	if (key == -1) {
		// reading the input directory, not temp
		std::vector<std::string> retVect;
		for (auto& input : std::filesystem::directory_iterator(directory)) {
			BOOST_LOG_TRIVIAL(info) << "Ingesting file: " << input.path();
			std::ifstream file(input.path());
			std::stringstream str;
			str << file.rdbuf();
			retVect.push_back(str.str());
		}
		return retVect;
	}
	else {
		std::string filePathName = directory;
		filePathName += "/file";
		filePathName += std::to_string(key);
		filePathName += ".txt";
		BOOST_LOG_TRIVIAL(info) << "Ingesting file: " << filePathName;
		std::ifstream file(filePathName);
		std::string line;
		while (std::getline(file, line)) {
			retVect.push_back(line);
		}
		return retVect;
	}
}

void FileManager::write(std::string word, std::string path, int key) {
	if (key == -1) {
		// final output file, not temp<key>
		std::ofstream outFile;
		outFile.open(std::string(path + "/final_output.txt"), std::ios_base::app);
		outFile << word;
		outFile.close();
	}
	else {
		std::ofstream outFile;
		outFile.open(std::string(path + "/file" + std::to_string(key) + ".txt"), std::ios_base::app);
		outFile << word;
		outFile.close();
	}
}

bool FileManager::init(std::string inDir, std::string tempDir, std::string outDir) {
	// check if input is a valid directory
	struct stat buffer;
	if (stat(inDir.c_str(), &buffer) != 0) {
		BOOST_LOG_TRIVIAL(error) << inDir << " directory does not exist!";
		return false;
	}
	else {
		// make new directories
		_mkdir(tempDir.c_str());
		_mkdir(outDir.c_str());
		// clean them in case they already exist
		for (const auto& entry : std::filesystem::directory_iterator(tempDir)) {
			std::filesystem::remove_all(entry.path());
		}
		for (const auto& entry : std::filesystem::directory_iterator(outDir)) {
			std::filesystem::remove_all(entry.path());
		}
		BOOST_LOG_TRIVIAL(info) << "FileSystem init success!";
		return true;
	}
}

void FileManager::printFinal(bool pass, std::string dir) {
	if (pass) {
		// print success file
		std::ofstream output(std::string(dir + "/SUCCESS"));
	}
	else {
		std::ofstream output(std::string(dir + "/FAILURE"));
	}
}

bool FileManager::test() {
	// unit test. writes file to local dir and reads it back
	try {
		_mkdir("test");
		write("\"test\", 1\n", "test", 0);
		std::vector<std::string> readback = read("test");
		if (strstr(readback.at(0).c_str(), "test") == nullptr) {
			std::filesystem::remove_all("test");
			return false;
		}
		std::filesystem::remove_all("test");
		return true;
	}
	catch (...) {
		std::filesystem::remove_all("test");
		return false;
	}
}