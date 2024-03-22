# pgx-collect

pgx-collect is a _nearly_ drop-in replacement for the pgx collection functions that offer
lower overhead.

pgx offers a very convenient set of functions for parsing values from rows, while handling
all of the boilerplate. However, these functions sometimes repeat significant amounts of
work when processing each row, and cause unnecessary allocations that scale with O(rows) 
or even O(rows * columns), which puts more load on the garbage collector. By rewriting 
these functions to minimize repeated work and allocations, this library provides the same 
convenient functionality while improving performance.

# Usage

For any Collect and RowTo function defined in pgx, pgx-collect has an equivalent with the
same name that you can swap in for the same behavior.

For example:

```golang
import (
    "github.com/jackc/pgx/v5"
)
...
values, err := pgx.CollectRows(rows, pgx.RowToStructByName[Record])
```

can be replaced with:

```golang
import (
    "github.com/jackc/pgx/v5"
    pgxc "github.com/zolstein/pgx-collect"
)
...
values, err := pgxc.CollectRows(rows, pgxc.RowToStructByName[Record])
```

If, when migrating from code using the pgx Collect functions, you need to reuse a custom
pgx.RowToFunc, you can use the Adapt function as follows. However, be aware that this may
have sub-par performance and allocate more than using native pgx-collect constructs.

```golang
import (
    "github.com/jackc/pgx/v5"
    pgxc "github.com/zolstein/pgx-collect"
)
...
var customFunc pgx.RowToFunc[Record]
values, err := pgxc.CollectRows(rows, pgxc.Adapt(customFunc))
```

# Who would benefit from this library?

On some level, everyone using the functions CollectRows or AppendRows could benefit.
The biggest wins will be seen by people parsing queries with many rows / columns 
using the reflective struct-mapping RowTo functions. I.e. RowToStructBy....

By comparison, people querying small numbers of rows and parsing rows as primatives
or maps, are unlikely to see much benefit (for now). pgx-collect support for these
operations is provided mainly for convenience, rather than because it can provide
a large speedup.

# How much faster is it?

When using AppendRows to query 1000 rows and 4 columns in a benchmark, I've demonstrated
a 3-4x reduction in total runtime, a 10x reduction in memory allocated,
and a 100x reduction in objects allocated. These numbers will vary greatly by workload.
Real-world benefits are unlikely to be this large, but they may still improve query
throughput, especially in pathological cases.

I encourage anyone using these pgx's collect functions to try out pgx-collect, measure
the difference, and share the results, particularly if there are further opportunities 
to optimize.

# Why not merge into pgx? Why create a separate library?

I would love for this code to be merged into pgx. However, even though the pgx-collect API
is designed as a drop-in replacement for all idiomatic use-cases, the performance improvements
required changing the types of some values in the public API - thus, if these changes were 
integrated into pgx, some working code could stop compiling.

If at some time in the future pgx can make backward-incompatible changes, I hope it
integrates these changes. I also hope they can integrate as many of the optimizations as
possible, though as far as I can tell the API exposed by pgx makes this impossible. In
lieu of this, I want this code to be available to anyone who wants to use it or learn from
it.
