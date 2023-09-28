/**
 * The entry files for the separated hooks
 */
// chunjun custom
import { i18n as userland } from "./../i18n/index.js";
import { I18N } from "./types/index.jsx";

/**
 * Calculates the default language from the user's language setting and the i18n object.
 * In case there is a language set in the browser which is also available as translation,
 * override the default language setting in the config file.
 * @returns string indicating the default language to use
 */
const getDefaultLanguage = (userI18n: I18N): string => {
  let browserLang: string = "";

  if (
    userI18n.useBrowserDefault &&
    typeof window !== "undefined" &&
    window &&
    window.navigator &&
    (window.navigator.languages || window.navigator.language)
  ) {
    browserLang = (
      (window.navigator.languages && window.navigator.languages[0]) ||
      window.navigator.language
    )
      .split("-")[0]
      .toLowerCase();
  }
  if (
    userI18n.useBrowserDefault &&
    browserLang &&
    userI18n.translations[browserLang]
  ) {
    return browserLang;
  }
  return userI18n.defaultLang;
};

/**
 * Imports the translations addes by the user in "i18n/index",
 * veryfies the tranlsations and exposes them
 * to the custom hooks
 * @returns the translations and the default language as defined in "i18n/index"
 */
const i18n = (): I18N | Error => {
  // cast to be typsafe
  const userI18n = userland as I18N;
  if (Object.keys(userI18n.translations).length < 1) {
    throw new Error(
      `Missing translations. Did you import and add the tranlations in 'i18n/index.js'?`
    );
  }
  if (userI18n.defaultLang.length === 0) {
    throw new Error(
      `Missing default language. Did you set 'defaultLang' in 'i18n/index.js'?`
    );
  }
  if (!userI18n.translations[userI18n.defaultLang]) {
    throw new Error(
      `Invalid default language '${userI18n.defaultLang}'. Check your 'defaultLang' in 'i18n/index.js'?`
    );
  }
  userI18n.defaultLang = getDefaultLanguage(userI18n);
  return userI18n;
};

export default i18n;
