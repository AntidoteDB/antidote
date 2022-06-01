---
layout: docs
title: Types
---

# Data Types

The following table shows the built-in / primitive Ecto types and how they can be stored in Antidote:

<div class="table-responsive tbody-primary-strongs" markdown="1">

| Ecto | Elixir | Antidote | Configurable alternative |
| --- | --- | --- | --- |
| Row (`Ecto.Schema`) | `%Struct{}` | Remove-As-Reset Map |  |
| `:id` | `integer` | ID* | Counter |
| `:binary_id` | `binary` | Last-Write-Wins Register | Multi-Value Register |
| `:integer` | `integer` | Last-Write-Wins Register | Multi-Value Register |
| `:float` | `float` | Last-Write-Wins Register | Multi-Value Register |
| `:boolean` | `boolean` | Enable-Wins Flag | Disable-Wins |
| `:string` | `UTF-8 encoded string` | Last-Write-Wins Register | Text editing CRDTs? |
| `:binary` | `binary` | Last-Write-Wins Register | Multi-Value Register |
| `{:array, inner_type}` | `list` | Replicated Growable Array |  |
| `:map` | `map` | Remove-As-Reset Map |  |
| `{:map, inner_type}` | `map` | Remove-As-Reset Map |  |
| `:decimal` | `Decimal` | Last-Write-Wins Register | Multi-Value Register |
| `:date` | `Date` | Last-Write-Wins Register | Multi-Value Register |
| `:time` | `Time` | Last-Write-Wins Register | Multi-Value Register |
| `:time_usec` | `Time` | Last-Write-Wins Register | Multi-Value Register |
| `:naive_datetime` | `NaiveDateTime` | Last-Write-Wins Register | Multi-Value Register |
| `:naive_datetime_usec` | `NaiveDateTime` | Last-Write-Wins Register | Multi-Value Register |
| `:utc_datetime` | `DateTime` | Last-Write-Wins Register | Multi-Value Register |
| `:utc_datetime_usec` | `DateTime` | Last-Write-Wins Register | Multi-Value Register |

</div>

*ID type does not yet exist. No current support. In addition, we expose Antidote CRDTs using custom Types provided by `Vax.Ecto.Types`.

## Todo

- specify how we store relationships
- identify types that we aspire to provide
