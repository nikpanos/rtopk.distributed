package model;

public enum ItemType {
	
	W_InTopK(0), S_antidom(1), S(2), W(3);
    private final int value;

    private ItemType(int value) {
        this.value = value;
    }

    
	public int getValue() {
		return value;
	}
}
