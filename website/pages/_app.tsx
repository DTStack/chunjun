import '@/styles/globals.css';
import '@/styles/font.css';
import 'aos/dist/aos.css';
import Head from 'next/head';
import type { AppProps } from 'next/app';
import { AppShell, MantineProvider, ColorSchemeProvider, ColorScheme } from '@mantine/core';
import { useState } from 'react';
import { useTranslation } from '@/next-export-i18n';
import AppHeader from '@/components/AppHeader';
import AppNavbar from '@/components/AppNavbar';

function MyHead() {
  const { t } = useTranslation();

  return (
    <Head>
      <title>{t('Home.title')}</title>
      <meta name='description' content='纯钧Chunjun' />
      <link rel='shortcut icon' href='chunjun/favicon.ico' />
    </Head>
  );
}

function MyApp({ Component, pageProps }: AppProps) {
  const [colorScheme, setColorScheme] = useState<ColorScheme>('dark');
  const [opened, setOpened] = useState<boolean>(false);
  const toggleColorScheme = () => setColorScheme(colorScheme === 'dark' ? 'light' : 'dark');
  const toggleNavbar = () => {
    setOpened((o) => !o);
  };
  return (
    <ColorSchemeProvider colorScheme={colorScheme} toggleColorScheme={toggleColorScheme}>
      <MantineProvider theme={{ colorScheme }} withGlobalStyles withNormalizeCSS>
        <MyHead />
        <AppShell
          padding={'md'}
          header={
            <AppHeader
              opened={opened}
              changeOpened={toggleNavbar}
              changeTheme={toggleColorScheme}
              theme={colorScheme}
            />
          }
          styles={(theme) => ({
            main: {
              backgroundColor:
                theme.colorScheme === 'dark' ? theme.colors.dark[4] : theme.colors.white,
              padding: 0
            }
          })}
        >
          <Component {...pageProps} />
          <AppNavbar opened={opened} changeOpened={toggleNavbar} />
        </AppShell>
      </MantineProvider>
    </ColorSchemeProvider>
  );
}

export default MyApp;
