package com.dtstack.flinkx.connector.api;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;

public class NoOPPlanner implements Planner {
    @Override
    public Parser getParser() {
        return null;
    }

    @Override
    public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        return null;
    }

    @Override
    public String explain(List<Operation> operations, ExplainDetail... extraDetails) {
        return null;
    }

    @Override
    public String getJsonPlan(List<ModifyOperation> list) {
        return null;
    }

    @Override
    public String explainJsonPlan(String s, ExplainDetail... explainDetails) {
        return null;
    }

    @Override
    public List<Transformation<?>> translateJsonPlan(String s) {
        return null;
    }

    public String[] getCompletionHints(String statement, int position) {
        return new String[0];
    }
}
