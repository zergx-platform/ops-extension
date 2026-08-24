declare module '*.css' {
  const content: string
  export default content
}

interface ImportMetaEnv {
  readonly RUCODER_SERVICE: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
