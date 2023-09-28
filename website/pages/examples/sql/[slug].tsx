import { getAllSqlFiles, getAllSqlPaths, getSqlByName } from '@/api/sql-api';
import { generateTree } from '@/utils/generateTree';
import DocumentLike from '@/common/document-like';
import FileTree from '@/types/FileTree';
import Params from '@/types/Params';
import { Prism } from '@mantine/prism';
import { GetStaticPropsContext } from 'next';

const SEP = process.env.sep as string;

type Props = {
  sql: string;
  tree: FileTree[];
};

const SqlExamples = (props: Props) => {
  const { sql, tree } = props;
  return (
    <DocumentLike tree={tree} target='/examples/sql'>
      <div className='md:col-span-4 md:px-12 md:py-8 p-2'>
        <Prism spellCheck language='sql'>
          {sql}
        </Prism>
      </div>
    </DocumentLike>
  );
};

export default SqlExamples;

export const getStaticPaths = async () => {
  const sqlFiles = getAllSqlFiles();
  const sqlCN = sqlFiles.map((name) => {
    return {
      params: {
        slug: name.slug.split('/').join(SEP)
      }
    };
  });
  const sqlEN = sqlFiles.map((name) => {
    return {
      params: {
        slug: name.slug.split('/').join(SEP)
      }
    };
  });

  return {
    paths: sqlCN.concat(sqlEN),
    fallback: false
  };
};

export const getStaticProps = async ({ params }: Params & GetStaticPropsContext) => {
  const sql = getSqlByName(params.slug);

  const allPaths = getAllSqlPaths();
  const tree = generateTree(allPaths);

  return {
    props: {
      sql: sql.content,
      tree
    }
  };
};
