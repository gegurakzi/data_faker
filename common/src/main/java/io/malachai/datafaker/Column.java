package io.malachai.datafaker;

public class Column {

    private String fullName;
    private String type;
    private String kind;
    private ConstraintType constraint;
    private String refers;
    private Column reference;

    public Column(String fullName, String type, String kind, ConstraintType constraint,
        String refers) {
        this.fullName = fullName;
        this.type = type;
        this.kind = kind;
        this.constraint = constraint;
        this.refers = refers;
    }

    public String getFullName() {
        return fullName;
    }

    public String getType() {
        return type;
    }

    public String getKind() {
        return kind;
    }

    public ConstraintType getConstraint() {
        return constraint;
    }

    public String getName() {
        return fullName.split("[.](?=[^.]*$)")[1];
    }

    public String getTableFullName() {
        return fullName.split("[.](?=[^.]*$)")[0];
    }

    public String getRefers() {
        return refers;
    }

    public Column getReference() {
        return reference;
    }

    public void setReference(Column column) {
        this.reference = column;
    }


}
