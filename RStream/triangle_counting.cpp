
#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/relation_phase.hpp"
#include "../core/global_info.hpp"
using namespace RStream;

struct RInUpdate_TriC : BaseUpdate {
	VertexId src;

	RInUpdate_TriC() : BaseUpdate () {
	}

	RInUpdate_TriC(VertexId t, VertexId s) : BaseUpdate(t){
		src = s;
	}
};

inline std::ostream & operator<<(std::ostream & strm, const RInUpdate_TriC& update){
	strm << "(" << update.target << ", " << update.src << ")";
	return strm;
}

struct ROutUpdate_TriC : BaseUpdate {
	VertexId src1;
	VertexId src2;

	ROutUpdate_TriC() : BaseUpdate () {
	}

	ROutUpdate_TriC(VertexId t, VertexId s1, VertexId s2) : BaseUpdate(t){
		src1 = s1;
		src2 = s2;
	}

};

inline std::ostream & operator<<(std::ostream & strm, const ROutUpdate_TriC& update){
	strm << "(" << update.target << ", " << update.src1 << ", " << update.src2 << ")";
	return strm;
}


RInUpdate_TriC* generate_one_update(Edge * e)
{
	RInUpdate_TriC* update = new RInUpdate_TriC(e->target, e->src);
	return update;
}


class R1 : public RPhase<RInUpdate_TriC, ROutUpdate_TriC> {
public:
	R1(Engine & e) : RPhase(e) {};
	~R1(){};


	bool filter(RInUpdate_TriC * update, VertexId edge_src, VertexId edge_dst) {
		if(update->src < update->target && edge_src < edge_dst)
			return false;

		return true;
	}

	ROutUpdate_TriC * project_columns(RInUpdate_TriC * in_update, VertexId edge_src, VertexId edge_dst) {
		ROutUpdate_TriC * new_update = new ROutUpdate_TriC(edge_dst, in_update->src, in_update->target);
		return new_update;
	}
};

class R2 : public RPhase<ROutUpdate_TriC, ROutUpdate_TriC> {
public:
	R2(Engine & e) : RPhase(e) {};
	~R2(){};

	bool filter(ROutUpdate_TriC * update, VertexId edge_src, VertexId edge_dst) {
		if(update->src1 != edge_dst) return true;
		return false;
	}

	ROutUpdate_TriC * project_columns(ROutUpdate_TriC * in_update, VertexId edge_src, VertexId edge_dst) {
		ROutUpdate_TriC * new_update = new ROutUpdate_TriC(in_update->target, in_update->src1, in_update->src2);
		return new_update;
	}
};

template<typename T>
void printUpdateStream(int num_partitions, std::string fileName, Update_Stream in_stream){
	for(int i = 0; i < num_partitions; i++) {
		std::cout << "--------------------" + (fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)) + "---------------------\n";
		int fd_update = open((fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)).c_str(), O_RDONLY);

		if(fd_update <= 0)
			continue;

		// get file size
		long update_file_size = io_manager::get_filesize(fd_update);

		char * update_local_buf = new char[update_file_size];
		io_manager::read_from_file(fd_update, update_local_buf, update_file_size, 0);

		// for each update
		for(size_t pos = 0; pos < update_file_size; pos += sizeof(T)) {
			// get an update
			T & update = *(T*)(update_local_buf + pos);
			std::cout << update << std::endl;
		}
	}
}


int main(int argc, char ** argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 0);
	std::cout << Logger::generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	// get running time (wall time)
	auto start = std::chrono::high_resolution_clock::now();

	//scatter phase first to generate updates
	std::cout << "\n\n" << Logger::generate_log_del(std::string("scatter"), 1) << std::endl;
	Scatter<BaseVertex, RInUpdate_TriC> scatter_phase(e);
	Update_Stream in_stream = scatter_phase.scatter_no_vertex(generate_one_update);

	//relational phase 1
	std::cout << "\n\n" << Logger::generate_log_del(std::string("first join"), 1) << std::endl;
	R1 r1(e);
	Update_Stream out_stream_1 = r1.join(in_stream);

	//relational phase 2
	std::cout << "\n\n" << Logger::generate_log_del(std::string("second join"), 1) << std::endl;
	R2 r2(e);
	Update_Stream out_stream_2 = r2.join(out_stream_1);

	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> diff = end - start;
	std::cout << "Finish triangle counting. Running time : " << diff.count() << " s\n";

	std::cout << "Triangle Counting : " << Global_Info::count(out_stream_2, sizeof(ROutUpdate_TriC), e) << std::endl;
}




