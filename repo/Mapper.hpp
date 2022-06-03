

#ifndef MAPPER_HPP
#define MAPPER_HPP

#include <vector>
#include <string>

class Mapper
{
public:
	explicit Mapper(int count=0);
	void map(int k, std::string c, std::string dir);
	static bool test();
	
private:

	void exporter(std::vector<std::string> vect, std::string temp, int key);
}
;





#endif MAPPER_H
