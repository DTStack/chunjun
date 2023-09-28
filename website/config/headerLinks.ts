import { LocaleType } from "@/api/post-api";

const SEP = process.env.sep as string;

export type headerLink = {
  name: string;
  path: string | headerLink[];
  key: string;
};

/**
 * Translate the Link path to English version
 * @param key The key field of Link object
 * @param defaultPath The path filed of Link object
 * @returns Return the translated path which matched by the key, if not return the defaultPath
 */
export function getLocaleLinkPath(key: string, defaultPath: string, locale: LocaleType = "zh"): string {
  if (locale === "en") {
    switch (key) {
      case "documents":
        return `/documents/en/Quick Start`;
      default:
        return defaultPath;
    }
  } else {
    switch (key) {
      case "documents":
        return `/documents/zh/快速开始`;
      default:
        return defaultPath;
    }
  }
}

export function transformPathname(path: string): string {
  return path.replace(/\?.*/, "");
}

export const headerLinks: headerLink[] = [
  {
    name: "首页",
    path: "/",
    key: "home",
  },
  {
    name: "文档",
    path: "/documents/zh/快速开始",
    key: "documents",
  },
  {
    name: "下载",
    path: "https://github.com/DTStack/chunjun/releases",
    key: "download",
  },
  {
    name: "指南",
    path: "/faq",
    key: "faq",
  },
  {
    name: "案例",
    path: [
      {
        name: "sql",
        path: `/examples/sql/binlog${SEP}binlog_stream`,
        key: "sql",
      },
      {
        name: "json",
        path: `/examples/json/binlog${SEP}binlog_hive`,
        key: "json",
      },
    ],
    key: "examples",
  },
  {
    name: "贡献者",
    path: "/contributor",
    key: "contributor",
  },
];
