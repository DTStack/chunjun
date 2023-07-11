import members from '@/githubInfo/members.json';
import Aos from 'aos';
import { Card, Text, Avatar, useMantineColorScheme } from '@mantine/core';
import { useEffect } from 'react';
import Image from 'next/image';
import AppFooter from '@/components/AppFooter';

const Contributor = () => {
  const { colorScheme } = useMantineColorScheme();
  useEffect(() => {
    Aos.init({
      duration: 1000
    });
  }, [colorScheme]);
  return (
    <>
      <main className='md:w-4/5 2xl:max-w-[60vw] mx-auto w-full md:py-8 py-6'>
        <Text
          className={`font-roboto mb-4 md:text-3xl text-base font-nunito capitalize flex items-center justify-center ${
            colorScheme === 'dark' ? 'text-yellow-300' : 'text-black'
          }`}
        >
          \(@^0^@)/ our contributors
          <Image
            src='/assets/svg/nice.svg'
            height={50}
            width={50}
            alt='thanks'
            priority
            style={{ marginRight: 5 }}
          />
        </Text>
        <Text className='mb-8 text-center font-bold'>期待你的加入</Text>
        <div className='grid md:grid-cols-4 grid-cols-1 md:gap-4 gap-2 md:p-0 px-4 mb-12'>
          {members.map((member, index) => {
            return (
              <Card
                data-aos='fade-up'
                data-aos-delay={150 + index * 20}
                key={member.avatar_url}
                p='lg'
                radius='sm'
                withBorder
                className={`hover:scale-110 transition-all duration-300 ${
                  colorScheme === 'light' ? 'bg-gray-50' : null
                }`}
              >
                <Card.Section className='flex justify-center items-center p-4 '>
                  <Avatar
                    src={member.avatar_url}
                    alt={member.html_url}
                    className='mx-auto'
                    radius='xl'
                    size='lg'
                    component='a'
                    href={member.html_url}
                  />
                </Card.Section>

                <Text
                  size='xl'
                  className={`text-center font-nunito ${
                    colorScheme === 'light' ? 'text-black' : 'text-gray-200'
                  }`}
                >
                  {member.login}
                </Text>
              </Card>
            );
          })}
        </div>
        <Text className='text-sm'>ps.以上排名不分先后</Text>
      </main>
      <AppFooter />
    </>
  );
};

export default Contributor;
