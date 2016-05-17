package hadoopUtils.counters;

public enum MyCounters {
	S,
	W,
	S1, //S_survive_by_domination,
	S_pruned_by_domination,
	W_in_RTOPk,
	W_pruned_by_GridS,
	W1,
	//S11,
	S2_pruned_by_GridW,
	S2_pruned_by_RLists,
	S2_pruned_by_RLists_in_Combiner,
	//Total_effort_to_load_GridS_in_seconds,
	//Total_effort_to_load_GridW_in_seconds,
	//Total_effort_for_pruning_S_in_MilliSeconds,
	//Total_effort_for_pruning_W_in_MilliSeconds,
	S2,
	W2,
	//Reducer_Input_WInTopK,
	RTOPk_Output,
	S_Elements_In_Antidominance_Area_Of_GridS,
	Reducers_Early_Terminated,
	Combiners_Early_Terminated
	//Total_effort_to_create_rtree_in_seconds,
	//Total_effort_for_rtopk_algorithm_in_seconds,
	//Total_effort_for_processing_w_in_rtopk_in_seconds,
}