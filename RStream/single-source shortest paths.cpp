	#include "../core/engine.hpp"
	#include "../core/scatter.hpp"
	#include "../core/gather.hpp"
	#include "../core/global_info.hpp"
	using namespace RStream;

	struct Update_SSSP : BaseUpdate{
		int path;

		Update_SSSP(int _target, int _path) : BaseUpdate(_target), path(_path) {};

		Update_SSSP() : BaseUpdate(0), path(0) {}

		std::string toString(){
			return "(" + std::to_string(target) + ", " + std::to_string(path) + ")";
		}
	}__attribute__((__packed__));

	struct Vertex_SSSP : BaseVertex {
		int degree;
		int path;
		int sum;
	}__attribute__((__packed__));

	void init(char* data, VertexId id) {
		struct Vertex_SSSP * v = (struct Vertex_SSSP*)data;
		v->degree = 0;
		v->sum = 0;
		v->path = 1;
		v->id = id;
	}

	Update_SSSP * generate_one_update_init(Edge * e, Vertex_SSSP * v) {
			Update_SSSP * update = new Update_SSSP(e->target, 1);
			return update;
		}

	Update_SSSP * generate_one_update(Edge * e, Vertex_SSSP * v) {
		Update_SSSP * update = new Update_SSSP(e->target, v->path);
		return update;
	}

	void apply_one_update(Update_SSSP * update, Vertex_SSSP * dst_vertex) {
        dst_vertex->sum = update->path > dst_vertex->path ? dst_vertex->path : update->path;
		dst_vertex->path += dst_vertex->sum;
	}


	int main(int argc, char ** argv) {
		Engine e(std::string(argv[1]), atoi(argv[2]), atoi(argv[3]));

		auto start = std::chrono::high_resolution_clock::now();

		std::cout << "--------------------Init Vertex--------------------" << std::endl;
		e.init_vertex<Vertex_SSSP>(init);
		std::cout << "--------------------Compute Degre--------------------" << std::endl;
		e.compute_degree<Vertex_SSSP>();

		int num_iters = 30;

		Scatter<Vertex_SSSP, Update_SSSP> scatter_phase(e);
		Gather<Vertex_SSSP, Update_SSSP> gather_phase(e);

		for(int i = 0; i < num_iters; i++) {
			std::cout << "--------------------Iteration " << i << "--------------------" << std::endl;

			Update_Stream in_stream;
			if(i == 0) {
				in_stream = scatter_phase.scatter_with_vertex(generate_one_update_init);
			} else {
				in_stream = scatter_phase.scatter_with_vertex(generate_one_update);
			}

			gather_phase.gather(in_stream, apply_one_update);

			Global_Info::delete_upstream(in_stream, e);
		}

		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff = end - start;
		std::cout << "Finish page rank. Running time : " << diff.count() << " s\n";

	}


