package com.dtstack.flinkx.connector.elasticsearch5.sink;

import com.dtstack.flinkx.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.google.common.base.Preconditions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 23:51
 */
public class ElasticsearchOutputFormatBuilder extends BaseRichOutputFormatBuilder {
    protected ElasticsearchOutputFormat format;

    public ElasticsearchOutputFormatBuilder() {
        super.format = format =  new ElasticsearchOutputFormat();
    }

    public void setEsConf(ElasticsearchConf esConf) {
        super.setConfig(esConf);
        format.setElasticsearchConf(esConf);
    }

    @Override
    protected void checkFormat() {
        ElasticsearchConf esConf = format.getElasticsearchConf();
        Preconditions.checkNotNull(esConf.getHosts(), "elasticsearch5 type of address is required");
        Preconditions.checkNotNull(esConf.getIndex(), "elasticsearch5 type of index is required");
        Preconditions.checkNotNull(esConf.getType(), "elasticsearch5 type of type is required");
        Preconditions.checkNotNull(esConf.getCluster(), "elasticsearch5 type of cluster is required");

        /**
         * is open basic auth
         */
        if (esConf.isAuthMesh()) {
            Preconditions.checkNotNull(esConf.getUserName(), "elasticsearch5 type of userName is required");
            Preconditions.checkNotNull(esConf.getPassword(), "elasticsearch5 type of password is required");
        }
    }

}
