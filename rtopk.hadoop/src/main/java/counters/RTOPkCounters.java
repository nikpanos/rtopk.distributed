package counters;

public enum RTOPkCounters {
	// �� �������� W ��� �������� �� Mappers
	TOTAL_W,
	// �� �������� S ��� �������� �� Mappers
	TOTAL_S,
	// �� S ��� ��������� ��� �� Dominance �� �� Q ����� Mappers 
	// (���� �� S_output_Map,S_Input_Reducer)
	DOMINACE_REJECTED_S,
	
	W_REJECTED_MAP,
	// �� W ��� ����� ������������ ��� RTOPk ����� Mappers
	//RTOPk_W_Map,
	// �� W ��� ���������� �� TopK ���� ��� RTOPk ����� Mappers
	//Examine_WTopK_Map,
	// �� �������� W ��� ����� �� output �� Mappers
	W_reject_Map,
	// �� �������� S ��� ����� �� output �� Mappers 
	// (���� �� DOMINACE_S,S_Input_Reducer)
	S_output_Map,
	// �� W ��� ����� ������������ ��� RTOPk ���� Reducer
	//RTOPk_W_Reduce,
	// �� W ��� ���������� �� TopK ���� ��� RTOPk ���� Reducer
	//Examine_WTopK_Reduce,
	// �� �������� W ��� �x�� �� input � Reducer	
	W_Input_Reducer,
	// �� �������� S ��� �x�� �� input � Reducer 
	// (���� �� DOMINACE_S,S_output_Map)
	//S_Input_Reducer,
	// �� �������� W ��� �x�� �� output � Reducer	
	W_output_Reducer;
}
