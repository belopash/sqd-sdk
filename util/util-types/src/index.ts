export type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

export type Selector<Props extends string, Exclusion> = {
    [P in Exclude<Props, Exclusion>]?: boolean
}

export type ExcludeUndefined<T> = {
    [K in keyof T as undefined extends T[K] ? never : K]: T[K]
} & {}

export type MergeDefault<T, D> = Simplify<
    undefined extends T ? D : Omit<D, keyof ExcludeUndefined<T>> & ExcludeUndefined<T>
>

export type TrueFields<F> = keyof {
    [K in keyof F as true extends F[K] ? K : never]: true
}

export type RemoveEmptyObjects<T> = {
    [K in keyof T as {} extends T[K] ? never : K]: T[K]
}
