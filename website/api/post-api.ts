import fs from "fs";
import { join } from "path";
import matter from "gray-matter";
import Items from "@/types/Item";

export type LocaleType = "zh" | "en";

//获取环境变量
const SEP = process.env.sep as string;
export const ROOT_ZH = process.env.root_zh as string;
export const ROOT_EN = process.env.root_en as string;

const postsDirectory = join(process.cwd(), ROOT_ZH);
const postsDirectoryEN = join(process.cwd(), ROOT_EN);

export function getLocaleSlug(key: string, defaultPath: string, locale: LocaleType = "zh"): string {
  if (locale === "en") {
    switch (key) {
      case "howContributing":
        return `Contribution${SEP}How to submit a great Pull Request`;
      case "howCustom":
        return `Contribution${SEP}How to define a plugin`;
      default:
        return defaultPath;
    }
  } else {
    switch (key) {
      case "howContributing":
        return `开发者指南${SEP}如何提交一个优秀的PR`;
      case "howCustom":
        return `开发者指南${SEP}如何自定义插件`;
      default:
        return defaultPath;
    }
  }
}

// 读取文件
export const getAllPaths = (root = ROOT_ZH, allFiles: string[] = []) => {
  const currentDir = join(process.cwd(), root);
  const files = fs.readdirSync(currentDir);

  for (const file of files) {
    if (file.includes(".md")) {
      allFiles.push(root === ROOT_ZH || root === ROOT_EN ? file : `${root.split("/").slice(3).join("/")}/${file}`);
    } else {
      getAllPaths(`${root}/${file}`, allFiles);
    }
  }

  return allFiles.filter((file) => file.includes(".md"));
};

export function getPostBySlug(slug: string, fields: string[] = [], locale: LocaleType = "zh") {
  const realSlug = slug.split(SEP).join("/").replace(/\.md$/, "");
  const rootPath = locale === "en" ? postsDirectoryEN : postsDirectory;

  const fullPath = join(rootPath, `${realSlug}.md`);
  const fileContents = fs.readFileSync(fullPath, "utf8");
  const { data, content } = matter(fileContents);
  const items: Items = {};

  fields.forEach((field) => {
    if (field === "slug") {
      items[field] = realSlug;
    }
    if (field === "content") {
      items[field] = content;
    }

    if (data[field]) {
      items[field] = data[field];
    }
  });
  return items;
}

export function getAllPosts(fields: string[] = [], locale: LocaleType = "zh") {
  const rootPath = locale === "en" ? ROOT_EN : ROOT_ZH;

  const slugs = getAllPaths(rootPath);
  const posts = slugs.map((slug) => getPostBySlug(slug, fields, locale));

  return posts;
}
