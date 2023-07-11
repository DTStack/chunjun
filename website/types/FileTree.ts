type FileTree = {
  label: string;
  category: "dir" | "file";
  children?: FileTree[];
  path?: string;
};

export default FileTree;
