import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';
import i18n from '../I18n';
import { I18N } from '../types';

/**
 * Returns a react-state containing the currently selected language.
 * @returns [lang as string, setLang as SetStateAction] a react-state containing the currently selected language
 */
export default function useSelectedLanguage()  {
	let i18nObj: I18N;

	i18nObj = i18n() as I18N;

	const defaultLang: string = i18nObj.defaultLang;
	const translations = i18nObj.translations;
	const router = useRouter();
	const [lang, setLang] = useState<string>(defaultLang);

	// set the language if the query parameter changes
	useEffect(() => {
		if (router.query.lang && router.query.lang !== lang && translations && translations[router.query.lang as string]) {
			let lang: string = router.query.lang as string;
			setLang(lang);
		}

	}, [lang, router.query.lang]);
	return { lang, setLang } as const;
	// return [lang, setLang] as const;
}
