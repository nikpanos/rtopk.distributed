package counters;

public enum RTOPkCounters {
	// Τα συνολικά W που διάβασαν οι Mappers
	TOTAL_W,
	// Τα συνολικά S που διάβασαν οι Mappers
	TOTAL_S,
	// Τα S που επιβίωσαν από το Dominance με το Q στους Mappers 
	// (ίδιο με S_output_Map,S_Input_Reducer)
	DOMINACE_REJECTED_S,
	
	W_REJECTED_MAP,
	// Τα W που είναι αποτελέσματα του RTOPk στους Mappers
	//RTOPk_W_Map,
	// Τα W που εξετάστηκε το TopK τους στο RTOPk στους Mappers
	//Examine_WTopK_Map,
	// Τα συνολικά W που έχουν ως output οι Mappers
	W_reject_Map,
	// Τα συνολικά S που έχουν ως output οι Mappers 
	// (ίδιο με DOMINACE_S,S_Input_Reducer)
	S_output_Map,
	// Τα W που είναι αποτελέσματα του RTOPk στον Reducer
	//RTOPk_W_Reduce,
	// Τα W που εξετάστηκε το TopK τους στο RTOPk στον Reducer
	//Examine_WTopK_Reduce,
	// Τα συνολικά W που έxει ως input ο Reducer	
	W_Input_Reducer,
	// Τα συνολικά S που έxει ως input ο Reducer 
	// (ίδιο με DOMINACE_S,S_output_Map)
	//S_Input_Reducer,
	// Τα συνολικά W που έxει ως output ο Reducer	
	W_output_Reducer;
}
