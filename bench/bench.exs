map = %{a: 1, b: 2, c: 3, d: 4}

Benchee.run(%{
  "Map.merge/2" => fn -> Map.merge(map, %{a: 5, b: 6}) end,
  "Comprehension" => fn -> %{map | a: 5, b: 6} end
})
