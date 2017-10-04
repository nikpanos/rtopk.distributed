package hadoopUtils.counters;

public enum MyCounters {
	W,   // To sunolo olou tou W dataset
	W_in_RTOPk,  // To plithos twn W pou anagnwristikan sth map fasi oti anikoun sto teliko result set
	W_pruned_by_GridS,  // To plithos twn W pou kopikan sth map fasi apo to grid S
	W1,  // To plithos twn W pou epezhsan sth map fasi
	RTOPk_Output,  //To plithos twn W pou anhkoun sto teliko result set
	W2_in_reducer,  //To plithos twn W pou diavastikan apo th reduce fasi (den periexei ta w pou xathikan logw tou early termination)
	W_topk_in_reducer,  // To plithos twn W pou elave h reduce fasi pou antistoixoun sta W_in_RTOPk
	
	S,   // To sunolo olou tou S dataset
	S1,  // To plithos twn S pou epezhsan apo to dominance
	S_pruned_by_domination,  // To plithos twn S pou kopikan apo to dominance
	S2_pruned_by_GridW,  // To plithos twn S2 pou kopikan sth map fasi apo to GridW (apo ta bounds diladi)
	S2_pruned_by_RLists,  // To plithos twn S2 pou kopikan sth map fasi apo ta RLists
	S2_pruned_by_Cell_Antidominance_Area,  // To plithos twn S2 pou kopikan sth map fasi epeidh ena partition exei hdh sumplhrwsei k antidominance items
	//Total_effort_to_load_GridS_in_seconds,
	//Total_effort_to_load_GridW_in_seconds,
	//Total_effort_for_pruning_S_in_MilliSeconds,
	//Total_effort_for_pruning_W_in_MilliSeconds,
	S2_by_mapper,  // To plithos twn S2 pou epezhsan apo ola ta filtra ths map fasis kai stalthikan sth reduce
	S2_by_mapper_antidominance_area,  // To plithos twn S2 pou anagnwristikan sth map fasi oti anikoun sthn antidominance area enos partition
	S2_in_reducer,  // To plithos twn S2_by_mapper pou diavastikan apo th reduce fasi (den periexei ta s pou xathikan logw tou early termination),
	S_antidom_in_reducer,  // To plithos twn S2_by_mapper_antidominance_area pou diavastikan apo th reduce fasi (den periexei ta s pou xathikan logw tou early termination),
	
	Reducers_Early_Terminated,  // To plithos twn reducers pou kanane early terminate
	
	
	
	
	S2_pruned_by_RLists_in_Combiner,
	S2_by_combiner,  //
	Combiners_Early_Terminated,
	S_Elements_In_Antidominance_Area_Of_GridS
	//Total_effort_to_create_rtree_in_seconds,
	//Total_effort_for_rtopk_algorithm_in_seconds,
	//Total_effort_for_processing_w_in_rtopk_in_seconds,
}