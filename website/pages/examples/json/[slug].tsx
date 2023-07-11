import { getAllJsonFiles, getAllJsonPaths, getJsonByName } from '@/api/json-api';
import { generateTree } from '@/utils/generateTree';
import DocumentLike from '@/common/document-like';
import FileTree from '@/types/FileTree';
import Params from '@/types/Params';
import { Prism } from '@mantine/prism';
import { GetStaticPropsContext } from 'next';

const SEP = process.env.sep as string;

type Props = {
  json: string;
  tree: FileTree[];
};

const JsonExamples = (props: Props) => {
  const { json, tree } = props;
  return (
    <DocumentLike tree={tree} target='/examples/json'>
      <div className='md:col-span-4 md:px-12 md:py-8 p-2'>
        <Prism spellCheck language='json'>
          {json}
        </Prism>
      </div>
    </DocumentLike>
  );
};

export default JsonExamples;

export const getStaticPaths = async () => {
  const jsonFiles = getAllJsonFiles();
  const pathCn = jsonFiles.map((name) => {
    return {
      params: {
        slug: name.slug.split('/').join(SEP)
      }
    };
  });
  const pathEN = jsonFiles.map((name) => {
    return {
      params: {
        slug: name.slug.split('/').join(SEP)
      }
    };
  });
  return {
    paths: pathCn.concat(pathEN),
    fallback: false
  };
};

export const getStaticProps = async ({ params }: Params & GetStaticPropsContext) => {
  const json = getJsonByName(params.slug);

  const allPaths = getAllJsonPaths();
  const tree = generateTree(allPaths);

  return {
    props: {
      json: json.content,
      tree
    }
  };
};
