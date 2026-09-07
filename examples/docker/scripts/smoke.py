"""Minimal Daft-on-Ray smoke test — assumes daft is baked into the image."""

import daft

daft.set_runner_ray()

df = daft.from_pydict(
    {
        "a": [3, 2, 5, 6, 1, 4],
        "b": [True, False, False, True, True, False],
    }
)
df = df.where(df["b"]).sort(df["a"])
print(df.collect())
