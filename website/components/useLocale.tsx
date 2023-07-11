import React, { useEffect, useState } from 'react'
import { LocaleType } from '@/api/post-api';
import { useLanguageQuery } from '@/next-export-i18n';

function useLocale() {
  const [query] = useLanguageQuery();
  const [locale, setLocale] = useState<LocaleType>('zh');

  useEffect(() => {
    setLocale(query?.lang === 'en' ? 'en' : 'zh');
  }, [query?.lang]);

  return locale
}

export default useLocale