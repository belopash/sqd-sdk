export type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

export type Selector<Props extends string> = {
    [P in Props]?: boolean
}

export type Select<T, S> = T extends any ? Pick<T, Extract<keyof T, ConditionalKeys<S, true>>> : never

export type ExcludeUndefined<T> = {
    [K in keyof T as undefined extends T[K] ? never : K]: T[K]
} & {}

export type MergeDefault<T, D> = Simplify<
    undefined extends T ? D : Omit<D, keyof ExcludeUndefined<T>> & ExcludeUndefined<T>
>

export type OverrideKeys<T, K> = Simplify<{
    [Key in keyof T | keyof K]: Key extends keyof K ? K[Key] : T[Key]
}>

export type ConditionalKeys<T, K> = {
    [Key in keyof T]-?: T[Key] extends K ? Key : never
}[keyof T]

export type RemoveEmptyObjects<T> = {
    [K in keyof T as {} extends T[K] ? never : K]: T[K]
}

export type AddPrefix<Prefix extends string, S> = S extends string ? `${Prefix}${Capitalize<S>}` : never

export type AddKeysPrefix<Prefix extends string, T> = {
    [K in keyof T as AddPrefix<Prefix, K>]: T[K]
}

export type RemovePrefix<Prefix extends string, T> = T extends `${Prefix}${infer S}` ? Uncapitalize<S> : never

export type RemoveKeysPrefix<Prefix extends string, T> = {
    [K in keyof T as RemovePrefix<Prefix, K>]: T[K]
}

export type LiteralUnion<T, K extends string> = T | (K & Record<never, never>)