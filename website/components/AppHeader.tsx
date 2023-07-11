import { Header, ActionIcon, ColorScheme, Text, Burger, Menu } from '@mantine/core';
import { Sun, Moon, ChevronDown } from 'tabler-icons-react';
import Image from 'next/image';
import { getLocaleLinkPath, headerLinks } from '@/config/headerLinks';
import Link from 'next/link';
import { useRouter } from 'next/router';
import { useTranslation, LanguageSwitcher } from '@/next-export-i18n';
import { LocaleType } from '@/api/post-api';
import useLocale from './useLocale';

type Props = {
  theme: ColorScheme;
  changeTheme: () => void;
  opened: boolean;
  changeOpened: () => void;
};

const AppHeader = (props: Props) => {
  const { theme, changeTheme, opened, changeOpened } = props;
  const router = useRouter();
  const { t } = useTranslation();
  const locale = useLocale();

  return (
    <Header
      height={64}
      className='flex items-center justify-between sticky shadow-md bg--gray-50 2xl:px-[22vw]'
    >
      <Burger opened={opened} onClick={changeOpened} className='md:hidden inline-block' />
      <div className='h-full flex items-center flex-1 cursor-pointer'>
        <div
          className='flex h-full items-center md:justify-start justify-center md:w-auto w-full'
          onClick={() => router.push('/')}
        >
          <Image priority src='/logo-dark.svg' height={48} width={48} alt='logo of chunjun' />
          <Text className='text-xl capitalize flex items-center font-nunito select-none'>
            Chunjun
          </Text>
        </div>
        <div className='h-full justify-center flex-1 md:flex hidden items-center'>
          {headerLinks.map((link) => {
            if (link.path[0] === '/' && !Array.isArray(link.path)) {
              return (
                <Link
                  key={link.key}
                  href={{
                    pathname: getLocaleLinkPath(link.key, link.path, locale as LocaleType),
                    query: {
                      lang: locale
                    }
                  }}
                >
                  <a className='font-nunito inline-block md:px-5 text-center'>
                    {t(`Header.${link.key}`)}
                  </a>
                </Link>
              );
            } else if (Array.isArray(link.path)) {
              return (
                <Menu
                  shadow='md'
                  width={120}
                  trigger='hover'
                  openDelay={100}
                  closeDelay={200}
                  key={link.key}
                  position='bottom'
                >
                  <Menu.Target>
                    <span className='flex items-center md:px-5 justify-center text-center'>
                      {t(`Header.${link.key}`)}
                      <ChevronDown size={16} className='ml-1' />
                    </span>
                  </Menu.Target>
                  <Menu.Dropdown>
                    {link.path.map((url) => {
                      return (
                        <Menu.Item key={url.key}>
                          <Link
                            href={{
                              pathname: url.path as string,
                              query: {
                                lang: locale
                              }
                            }}
                          >
                            <a className='uppercase inline-block w-full'>{t(`Header.${url.key}`)}</a>
                          </Link>
                        </Menu.Item>
                      );
                    })}
                  </Menu.Dropdown>
                </Menu>
              );
            } else {
              return (
                <a
                  href={getLocaleLinkPath(link.key, link.path, locale as LocaleType)}
                  key={link.key}
                  className='font-raleway inline-block md:px-5 text-center'
                  target='blank'
                >
                  {t(`Header.${link.key}`)}
                </a>
              );
            }
          })}
        </div>
      </div>

      <div className='h-full flex items-center pr-[9px]'>
        <span className='inline-block mr-4'>
          {locale === 'en' ? (
            <LanguageSwitcher lang='zh'>简体中文</LanguageSwitcher>
          ) : (
            <LanguageSwitcher lang='en'>English</LanguageSwitcher>
          )}
        </span>
        <ActionIcon
          variant='outline'
          color={theme === 'dark' ? 'yellow' : 'blue'}
          onClick={changeTheme}
        >
          {theme === 'light' ? <Sun /> : <Moon />}
        </ActionIcon>
      </div>
    </Header>
  );
};

export default AppHeader;
