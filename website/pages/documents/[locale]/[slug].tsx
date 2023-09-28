import PostBody from '@/components/PostBody';
import { markdownToHtml } from '@/utils/markdown2html';
import {
  getAllPosts,
  getPostBySlug,
  getAllPaths,
  ROOT_EN,
  ROOT_ZH,
  LocaleType
} from '@/api/post-api';
import { Skeleton } from '@mantine/core';
import { useRouter } from 'next/router';
import DocumentLike from '@/common/document-like';
import FileTree from '@/types/FileTree';
import Toc from '@/types/Toc';
import Params from '@/types/Params';
import { generateTree } from '@/utils/generateTree';
import { GetStaticPropsContext } from 'next';
import { useLanguageQuery } from '@/next-export-i18n';

const SEP = process.env.sep as string;

const Post = ({ content, tree, toc }: { content: string; tree: FileTree[]; toc: Toc[] }) => {
  const router = useRouter();
  const [query] = useLanguageQuery();
  const locale = query?.lang === 'en' ? 'en' : 'zh'

  return (
    <>
      <DocumentLike tree={tree} target={`/documents/${locale}`} toc={toc}>
        {router.isFallback ? (
          <Skeleton visible className='md:col-span-4' />
        ) : (
          <PostBody content={content} />
        )}
      </DocumentLike>
    </>
  );
};

export default Post;

export async function getStaticPaths() {
  const posts = getAllPosts(['slug']);
  const postsEN = getAllPosts(['slug'], 'en');
  const paths = posts.map((post) => {
    return {
      params: {
        locale: 'zh',
        slug: post.slug.split('/').join(SEP)
      }
    };
  });

  const pathsEn = postsEN.map((post) => {
    return {
      params: {
        locale: 'en',
        slug: post.slug.split('/').join(SEP)
      }
    };
  });
  return {
    paths: paths.concat(pathsEn),
    fallback: false
  };
}

export async function getStaticProps({ params }: Params & GetStaticPropsContext) {
  const post = getPostBySlug(params.slug, ['slug', 'content'], params.locale as LocaleType);
  const { content, toc } = await markdownToHtml(post.content || '');

  const rootPath = params.locale === 'en' ? ROOT_EN : ROOT_ZH;
  const allPaths = getAllPaths(rootPath);
  const tree = generateTree(allPaths);
  
  return {
    props: {
      content,
      tree,
      toc
    }
  };
}
