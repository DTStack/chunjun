package com.dtstack.flinkx.redis;

/**
 * @author jiangbo
 * @date 2018/6/29 15:44
 */
public enum ClusterModel {

    STANDALONE(0),SHARDED(1),CLUSTER(2);

    private int model;

    ClusterModel(int model) {
        this.model = model;
    }

    public int getModel() {
        return model;
    }

    public static ClusterModel getClusterModel(int model){
        for (ClusterModel clusterModel : ClusterModel.values()) {
            if(clusterModel.getModel() == model){
                return clusterModel;
            }
        }

        return STANDALONE;
    }
}
