#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/scatter_updates.hpp"
#include "../core/relation_phase.hpp"
#include "../core/global_info.hpp"
using namespace RStream;

struct Update_Stream_TC : BaseUpdate{
	VertexId src;
	Update_Stream_TC() : BaseUpdate() {
		src = 0;
		target = 0;
	}

	Update_Stream_TC(VertexId _src, VertexId _target) : BaseUpdate(_target)
	{
		src = _src;
	}

	bool operator == (const Update_Stream_TC & obj) const {
		if(src == obj.src && target == obj.target)
			return true;
		else
			return false;
	}
};
typedef Update_Stream_TC In_Update_TC;
typedef Update_Stream_TC Out_Update_TC;

namespace std {
	template<>
	struct hash<Update_Stream_TC> {
		size_t operator() (const Update_Stream_TC & obj) const {
			size_t hash = 17;
			hash = 31 * hash + obj.src;
			hash = 31 * hash + obj.target;
			return hash;
		}
	};
}

Out_Update_TC * generate_one_update(Edge * e) {
	Out_Update_TC * out_update = new Out_Update_TC(e->src, e->target);
	return out_update;
}

Out_Update_TC * generate_out_update(In_Update_TC * in_update) {
	Out_Update_TC * out_update = new Out_Update_TC(in_update->src, in_update->target);
	return out_update;
}

class TC : public RPhase<In_Update_TC, Out_Update_TC> {
	public:
		TC(Engine & e) : RPhase(e) {};
		~TC(){};

		bool filter(In_Update_TC * update, VertexId edge_src, VertexId edge_target) {
			if(update->src == edge_target)
				return true;

			return false;
		}

		Out_Update_TC * project_columns(In_Update_TC * in_update, VertexId edge_src, VertexId edge_target) {
			Out_Update_TC * new_update = new Out_Update_TC(in_update->src, edge_target);
			return new_update;
		}
};

bool should_terminate(Update_Stream delta_tc, Engine & e) {
	for(int i = 0; i < e.num_partitions; i++) {
		int fd_update = open((e.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(delta_tc)).c_str(), O_RDONLY);

		if(io_manager::get_filesize(fd_update) > 0) {
			close(fd_update);
			return false;
		}

		close(fd_update);
	}

	return true;
}

inline std::ostream & operator<<(std::ostream & strm, const In_Update_TC& update){
	strm << "(" << update.src << ", " << update.target << ")";
	return strm;
}

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

	auto start = std::chrono::high_resolution_clock::now();
	//scatter phase first to generate updates
	// update0
	std::cout << "\n\n" << Logger::generate_log_del(std::string("scatter to generate delta_tc"), 1) << std::endl;
	Scatter<BaseVertex, In_Update_TC> scatter_edges(e);
	Update_Stream delta_tc = scatter_edges.scatter_no_vertex(generate_one_update);


	// tc = update1
	std::cout << "\n\n" << Logger::generate_log_del(std::string("scatter to generate tc"), 1) << std::endl;
	Update_Stream tc = scatter_edges.scatter_no_vertex(generate_one_update);

	Scatter_Updates<In_Update_TC, Out_Update_TC> sc_up(e);
	TC triangle_counting(e);

	while(!should_terminate(delta_tc, e)) {

		std::cout << "\n\n" << Logger::generate_log_del(std::string("Iteration"), 1) << std::endl;
		// tmp with dup = update2
		std::cout << "\n\n" << Logger::generate_log_del(std::string("join delta_tc"), 2) << std::endl;
		Update_Stream tmp = triangle_counting.join(delta_tc);
		Global_Info::delete_upstream(delta_tc, e);

		// out without dup = update3
		std::cout << "\n\n" << Logger::generate_log_del(std::string("remove duplicates"), 2) << std::endl;
		Update_Stream out = triangle_counting.remove_dup(tmp);
		Global_Info::delete_upstream(tmp, e);

		// delta with set diff = update4
		std::cout << "\n\n" << Logger::generate_log_del(std::string("set difference to generate delta"), 2) << std::endl;
		Update_Stream delta = triangle_counting.set_difference(out, tc);
		Global_Info::delete_upstream(out, e);

		// union = upadte1 = tc
		std::cout << "\n\n" << Logger::generate_log_del(std::string("union delat with tc"), 2) << std::endl;
		triangle_counting.union_relation(tc, delta);

		delta_tc = delta;
	}

		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff = end - start;
		std::cout << "Finish Transitive closure. Running time : " << diff.count() << " s\n";
		std::cout << "Transitive closure : " << Global_Info::count(tc, sizeof(Out_Update_TC), e) << std::endl;
}



