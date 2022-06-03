#include <string>
#include <vector>
#include <sys/stat.h>
#include <direct.h>
#include <filesystem>

#pragma once

class FileManager {
public:
	static std::vector<std::string> read(std::string directory, int key = -1);
	static void write(std::string word, std::string path, int key = -1);
	static bool init(std::string, std::string, std::string);
	static void printFinal(bool pass, std::string dir);
	static bool test();
};
