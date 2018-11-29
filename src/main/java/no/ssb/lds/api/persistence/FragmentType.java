package no.ssb.lds.api.persistence;

public enum FragmentType {
    NULL((byte) 0), DELETED((byte) 1), EMPTY_OBJECT((byte) 2), EMPTY_ARRAY((byte) 3), STRING((byte) 4), NUMERIC((byte) 5), BOOLEAN((byte) 6);

    final byte typeCode;

    FragmentType(byte typeCode) {
        this.typeCode = typeCode;
    }

    public byte getTypeCode() {
        return typeCode;
    }

    public static FragmentType fromTypeCode(byte typeCode) {
        switch (typeCode) {
            case 0:
                return NULL;
            case 1:
                return DELETED;
            case 2:
                return EMPTY_OBJECT;
            case 3:
                return EMPTY_ARRAY;
            case 4:
                return STRING;
            case 5:
                return NUMERIC;
            case 6:
                return BOOLEAN;
        }
        throw new IllegalArgumentException("typeCode not supported: " + typeCode);
    }
}
