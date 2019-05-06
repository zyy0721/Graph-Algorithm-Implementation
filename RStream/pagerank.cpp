	#include "../core/engine.hpp"
	#include "../core/scatter.hpp"
	#include "../core/gather.hpp"
	#include "../core/global_info.hpp"
	using namespace RStream;

	struct Update_PR : BaseUpdate{
		float rank;

		Update_PR(int _target, float _rank) : BaseUpdate(_target), rank(_rank) {};

		Update_PR() : BaseUpdate(0), rank(0.0) {}

		std::string toString(){
			return "(" + std::to_string(target) + ", " + std::to_string(rank) + ")";
		}
	}__attribute__((__packed__));

	struct Vertex_PR : BaseVertex {
		int degree;
		float rank;
		float sum;
	}__attribute__((__packed__));

	void init(char* data, VertexId id) {
		struct Vertex_PR * v = (struct Vertex_PR*)data;
		v->degree = 0;
		v->sum = 0;
		v->rank = 1.0f;
		v->id = id;
	}

	Update_PR * generate_one_update_init(Edge * e, Vertex_PR * v) {
			Update_PR * update = new Update_PR(e->target, 1.0f / v->degree);
			return update;
		}

	Update_PR * generate_one_update(Edge * e, Vertex_PR * v) {
		Update_PR * update = new Update_PR(e->target, v->rank / v->degree);
		return update;
	}

	void apply_one_update(Update_PR * update, Vertex_PR * dst_vertex) {
		dst_vertex->sum += update->rank;
		dst_vertex->rank = 0.15 + 0.85 * dst_vertex->sum;
	}


	int main(int argc, char ** argv) {
		Engine e(std::string(argv[1]), atoi(argv[2]), atoi(argv[3]));

		auto start = std::chrono::high_resolution_clock::now();

		std::cout << "--------------------Init Vertex--------------------" << std::endl;
		e.init_vertex<Vertex_PR>(init);
		std::cout << "--------------------Compute Degre--------------------" << std::endl;
		e.compute_degree<Vertex_PR>();

		int num_iters = 30;

		Scatter<Vertex_PR, Update_PR> scatter_phase(e);
		Gather<Vertex_PR, Update_PR> gather_phase(e);

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


