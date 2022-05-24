# Vax
Client and Ecto adapter for Vaxine.

## Installation
The package is still not available on hex, so it can be installed by adding it
as a git dependency in your `mix.exs`:
```elixir
# mix.exs
{:vax, git: "https://github.com/vaxine-io/vax.git"}
```
After that, you can create a repo, such as:
```elixir
# lib/my_app/repo.ex
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Vax.Adapter
end
```
And the config for this repo:
```elixir
# config/dev.exs
config :my_app, MyApp.Repo, address: "localhost", port: 8087
```
And you're ready to go and use Vax!

## Usage
You can create a schema by using `Vax.Schema`.
```elixir
defmodule MyApp.Blog.Post do
  use Vax.Schema
  import Ecto.Changeset

  schema "posts" do
    field :title, :string
    field :likes, Vax.Types.Counter
  end
end
```
Fields are, by default, LWW registers. Custom vax types provide types that have
more interesting properties. For example, `Vax.Types.Counter` uses a
**Positive-negative CRDT counter**. You can read more about CRDTs on
[our website](https://vaxine.io).

Once you have a schema, you can use the regular `Repo` functions. Please
note that this is a work in progress and many features are still not supported!

## License
Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
