import Toc from '@/types/Toc'
import { useMantineColorScheme } from '@mantine/core'
type Props = {
  toc: Toc[]
}

const TableOfContent = (props: Props) => {
  const { toc } = props
  const { colorScheme } = useMantineColorScheme()
  return (
    <aside className="md:inline-block hidden relative">
      <div
        className={`sticky top-[100px] h-[100vh-64px] overflow-y-auto scrollbar ${
          colorScheme === 'light' ? 'border-l-gray-200' : 'border-l-gray-500'
        } border-l-[1px]`}
      >
        <ul className="w-full pl-4">
          {toc
            .filter((t) => t.level <= 2)
            .map((t) => (
              <li key={`${t.id}`} className={`pl-${t.level * 2}`}>
                <a
                  href={`#${t.text}`}
                  className={`font-raleway text-sm ${
                    colorScheme === 'light'
                      ? 'hover:text-indigo-600'
                      : 'hover:text-indigo-400'
                  }`}
                >
                  {t.text}
                </a>
              </li>
            ))}
        </ul>
      </div>
    </aside>
  )
}

export default TableOfContent
