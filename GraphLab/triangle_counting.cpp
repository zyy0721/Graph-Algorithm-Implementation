#include <boost/unordered_set.hpp>
#include <graphlab.hpp>
#include <graphlab/ui/metrics_server.hpp>
#include <graphlab/macros_def.hpp>

 //Each vertex maintains a list of all its neighbors and a final count for the number of triangles it is involved in
struct vertex_data_type {
  vertex_data_type():num_triangles(0) { }
  // A list of all its neighbors
  boost::unordered_set<graphlab::vertex_id_type> vid_set;
  // The number of triangles this vertex is involved it.
  size_t num_triangles;
  
  void save(graphlab::oarchive &oarc) const {
    oarc << vid_set << num_triangles;
  }
  void load(graphlab::iarchive &iarc) {
    iarc >> vid_set >> num_triangles;
  }
};


//Each edge is simply a counter of triangles
typedef size_t edge_data_type;



/*
 * This is the gathering type which accumulates an unordered set of all neighboring vertices.
 */
struct set_union_gather {
  boost::unordered_set<graphlab::vertex_id_type> vid_set;

  //Combining with another collection of vertices.
  //Union it into the current set.
  set_union_gather& operator+=(const set_union_gather& other) {
    foreach(graphlab::vertex_id_type othervid, other.vid_set) {
      vid_set.insert(othervid);
    }
    return *this;
  }
  
  // serialize
  void save(graphlab::oarchive& oarc) const {
    oarc << vid_set;
  }

  // deserialize
  void load(graphlab::iarchive& iarc) {
    iarc >> vid_set;
  }
};

// The graph type is determined by the vertex type and edge type
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;


/**
 * Implementation of Triangle Counting Algorithm in GAS model
 */
class triangle_count :
      public graphlab::ivertex_program<graph_type, set_union_gather>,
      /*if no data, just force it to POD */
      public graphlab::IS_POD_TYPE  {
public:
  // Gather on all edges
  edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::ALL_EDGES;
  } 

  /*
   * For each edge, figure out the ID of the "other" vertex
   * and accumulate a set of the neighborhood vertex IDs.
   */
  gather_type gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
    set_union_gather gather;
    /*Insert the opposite end of the edge IF the opposite end has
    * ID greater than the current vertex
    */
    vertex_id_type otherid = edge.source().id() == vertex.id() ?
                             edge.target().id() : edge.source().id();
    if (otherid > vertex.id()) 
        gather.vid_set.insert(otherid);
    return gather;
  }

  /*
   * the gather result now contains the vertex IDs in the neighborhood.
   * store it on the vertex. 
   */
  void apply(icontext_type& context, vertex_type& vertex, const gather_type& neighborhood) {
    vertex.data().vid_set = neighborhood.vid_set;
  }

  //Scatter over all edges to compute the intersection.
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
    return graphlab::OUT_EDGES;
  }


  //Computes the size of the intersection of two unordered sets
  static size_t count_set_intersect(
               const boost::unordered_set<vertex_id_type>& smaller_set,
               const boost::unordered_set<vertex_id_type>& larger_set) {
    size_t count = 0;
    foreach(vertex_id_type vid, smaller_set) {
      count += larger_set.count(vid);
    }
    return count;
  }

  /*
   * For each edge, count the intersection of the neighborhood of the
   * adjacent vertices. This is the number of triangles this edge is involved
   * in.
   */
  void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
    const vertex_data_type& srclist = edge.source().data();
    const vertex_data_type& targetlist = edge.target().data();
    if (srclist.vid_set.size() >= targetlist.vid_set.size()) {
      edge.data() = count_set_intersect(targetlist.vid_set, srclist.vid_set);
    }
    else {
      edge.data() = count_set_intersect(srclist.vid_set, targetlist.vid_set);
    }
  }
};

/* Used to sum over all the edges in the graph in a map_reduce_edges call
 * to get the total number of triangles
 */
size_t get_edge_data(const graph_type::edge_type& e) {
  return e.data();
}



int main(int argc, char** argv) {
  // Initialize control plane using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_DEBUG);
  graphlab::launch_metric_server();
  //parse options
  graphlab::command_line_options clopts("Exact Triangle Counting. ");
  std::string prefix, format;
  std::string per_vertex;
  clopts.attach_option("graph", prefix, "Graph input. reads all graphs matching prefix*");
  clopts.attach_option("format", format, "The graph format");
  
  if(!clopts.parse(argc, argv)) return EXIT_FAILURE;
  if (prefix == "") {
    std::cout << "--graph is not optional\n";
    clopts.print_description();
    return EXIT_FAILURE;
  }
  else if (format == "") {
    std::cout << "--format is not optional\n";
    clopts.print_description();
    return EXIT_FAILURE;
  }

  //Build the graph   
  graph_type graph(dc, clopts);   
  //load graph
  if (graph_dir.length() > 0) { // Load the graph from a file
      dc.cout() << "Loading graph in format: "<< format << std::endl;
      graph.load_format(graph_dir, format);
  } else {
      dc.cout() << "graph must be specified" << std::endl;
      clopts.print_description();
      return EXIT_FAILURE;
  }
  // must call finalize before querying the graph
  graph.finalize();
  dc.cout() << "Number of vertices: " << graph.num_vertices() << std::endl
            << "Number of edges:    " << graph.num_edges() << std::endl;

  graphlab::timer ti;
  
  // create engine to count the number of triangles
  dc.cout() << "Counting Triangles..." << std::endl;
  //running the engine
  graphlab::synchronous_engine<triangle_count> engine(dc, graph, clopts);
  engine.signal_all();
  engine.start();

  dc.cout() << "Counted in " << ti.current_time() << " seconds" << std::endl;
   
  graphlab::stop_metric_server();
  //Tear-down communication layer and quit
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
}

