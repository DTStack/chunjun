import { markdownToHtml } from '@/utils/markdown2html';
import { getLocaleSlug, getPostBySlug, LocaleType } from '@/api/post-api';
import MDFileType from '@/types/MDFile';
import PostBody from '@/components/PostBody';
import AppFooter from '@/components/AppFooter';
import { Text, Blockquote, Tabs, Button, useMantineColorScheme } from '@mantine/core';
import { BrandGithub, Affiliate } from 'tabler-icons-react';
import Aos from 'aos';
import { useEffect } from 'react';
import { GetStaticPropsContext } from 'next';
import { useTranslation } from '@/next-export-i18n';

const SEP = process.env.sep as string;

type Props = {
  PR: MDFileType;
  plugin: MDFileType;
};

const Faq = (props: Props) => {
  const { PR, plugin } = props;
  const { colorScheme } = useMantineColorScheme();
  const { t } = useTranslation();

  useEffect(() => {
    Aos.init();
  }, [colorScheme]);

  return (
    <>
      <div className='md:w-4/5 2xl:w-[55vw] w-full mx-auto py-8'>
        <Text
          data-aos='fade-zoom-in'
          className='px-4 md:py-12 md:text-3xl text-base capitalize md:mb-12 mb-8 font-bold'
        >
          {t('Faq.introduction')}
        </Text>
        <div id='top' className='h-8'></div>
        <Tabs variant='outline' defaultValue='pr' className='mb-8'>
          <Tabs.List position='center'>
            <Tabs.Tab value='pr' icon={<BrandGithub size={14} />}>
              {t('Faq.pr')}
            </Tabs.Tab>
            <Tabs.Tab value='plugin' icon={<Affiliate size={14} />}>
              {t('Faq.plugin')}
            </Tabs.Tab>
          </Tabs.List>

          <Tabs.Panel value='pr' pt='xs'>
            <Blockquote cite='– 如何提交一个优秀的PR'>
              <PostBody content={PR.content} />
            </Blockquote>
          </Tabs.Panel>

          <Tabs.Panel value='plugin' pt='xs'>
            <Blockquote cite='– 如何自定义插件'>
              <PostBody content={plugin.content} />
            </Blockquote>
          </Tabs.Panel>
        </Tabs>
        <div className='text-center'>
          <Button component='a' href='#top' variant='light' className='capitalize'>
            回 到 顶 部
          </Button>
        </div>
      </div>
      <AppFooter />
    </>
  );
};

export default Faq;

export const getStaticProps = async ({}: GetStaticPropsContext) => {
  const locale = 'zh';
  const post1 = getPostBySlug(
    getLocaleSlug(
      'howContributing',
      `开发者指南${SEP}如何提交一个优秀的PR'}`,
      locale as LocaleType
    ),
    ['slug', 'content'],
    locale as LocaleType
  );
  const md1 = await markdownToHtml(post1.content || '');

  const post2 = getPostBySlug(
    getLocaleSlug('howCustom', `开发者指南${SEP}如何自定义插件`, locale as LocaleType),
    ['slug', 'content'],
    locale as LocaleType
  );
  const md2 = await markdownToHtml(post2.content || '');

  return {
    props: {
      PR: md1,
      plugin: md2
    }
  };
};
