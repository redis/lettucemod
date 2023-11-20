package com.redis.search.query.filter;

public class VectorField extends AbstractField {

    public VectorField(String name) {
	super(name);
    }

    public FieldCondition range(Number radius, String vectorParam) {
	return new FieldCondition(this, new VectorRangeCondition(radius, vectorParam));
    }

    public VectorKNNCondition knn(int num, String vectorParam) {
	return new VectorKNNCondition(this, num, vectorParam);
    }

    public VectorKNNCondition knn(String numParam, String vectorParam) {
	return new VectorKNNCondition(this, numParam, vectorParam);
    }

}
