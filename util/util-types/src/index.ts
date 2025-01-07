export type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

export type Schema<T extends {[key in K]: any | undefined}, K extends string = never> = T

export type Selector<Props extends string> = {
    [P in Props]?: boolean
}

export type Select<T, F> = T extends any ? Pick<T, Extract<keyof T, F>> : never

export type ExcludeUndefined<T> = {
    [K in keyof T as undefined extends T[K] ? never : K]: T[K]
} & {}

export type MergeDefault<T, D> = Simplify<
    undefined extends T ? D : Omit<D, keyof ExcludeUndefined<T>> & ExcludeUndefined<T>
>

export type ConditionalKeys<T, K> = keyof {
    [Key in keyof T]-?: T[Key] extends K ? T[Key] : never
}

export type RemoveEmptyObjects<T> = {
    [K in keyof T as {} extends T[K] ? never : K]: T[K]
}

export type AddPrefix<Prefix extends string, S extends string> = `${Prefix}${Capitalize<S>}`

export type RemovePrefix<Prefix extends string, T> = T extends `${Prefix}${infer S}` ? Uncapitalize<S> : never

export type LiteralUnion<T, K extends string> = T | (K & Record<never, never>)
