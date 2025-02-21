<script lang="ts">
    import axios, { AxiosError, type AxiosResponse } from "axios";
    import { onMount } from "svelte";
    import { fade, fly } from "svelte/transition";
    import { pipeline, env } from "@xenova/transformers";
    import { error } from "@sveltejs/kit";

    env.allowLocalModels = false;
    const pipe = pipeline(
      "feature-extraction",
      "caiodallaqua/vectoria-demo-all-MiniLM-L6-v2"
    );
  
    let errorMsg = "";

    let term = "";

    let photos: {
      id: string;
    }[] = [];
  
    let k = 10;
    let threshold = 0.5;
  
    const addr = "http://localhost:8558"

    const configServer = async () => {
      return await axios.get(`${addr}/system/health`)
        .catch((err: AxiosError) => {
          errorMsg = "Oops! Unable to connect to the server."
          console.log("server health returned error:", err.message)
        });
    };

    const fetchData = async () => {
      pipe
        .then((encode) => {
          return encode(term || "cat", { pooling: "mean", normalize: true });
        })
        .then((embeddings) => {
          const jsonArray: Array<number> = Object.values(embeddings.data).map(
            Number
          );
  
          const response = axios
            .post(`${addr}/get`, {
              index_name: "demo",
              query: jsonArray,
              threshold: threshold,
              k: k,
            })
            .then((res) => {
              photos = res.data.ids.map((id: string) => ({ id: id }));
            })
            .catch((err) => {
              errorMsg = "Oops! Unable to fetch data from server."
              console.error("could not fetch data from server", err.message);
            });
        })
        .catch((err: AxiosError) => {
          errorMsg = "Oops! We had a problem when encoding the text. This is likely a client-side issue."
          console.error("could not encode search term", err.message);
        });
    };


    let search_ref: any;
    
    onMount(() => {
      search_ref.focus(); 
      configServer().then(
        () => fetchData()
      );
    });
  
    const handleSearch = async () => {
      if (!term) return;
      await fetchData();
      // term = "";
    };
  </script>
  
  <div class="container">
    <div class="header">
      <h1>Vectoria Demo</h1>
      <form class="input-container">
        <div class="parameters">
          <div class="threshold">
            threshold = {threshold}
            <input type="range" bind:value={threshold} min="0" max="1" step="0.1" />
          </div>
          <div class="k">
            k = {k}
            <!-- {#if k === 0}
              (return all neighbors)
            {/if} -->
            <input type="range" bind:value={k} min="0" max="20" />
          </div>
        </div>
        <div class="search-bar">
          <input type="text" class="search-textbox" bind:value={term} bind:this={search_ref}/>
          <button class="search-button" on:click={handleSearch}>
            <img src="search.png" alt="search"/>
          </button>
        </div>
      </form>
    </div>
    {#if errorMsg}
      <div class="error-message">
        {errorMsg}
      </div>
    {:else}
      <div class="photos">
        {#each photos as photo, i (photo.id)}
          <img
            src={photo.id}
            alt=""
            class="image"
            in:fly={{ y: 200, duration: 2000, delay: i * 200 }}
            out:fade
          />
        {/each}
      </div>
    {/if}
  </div>
  
  <style>
    * {
      margin: 0;
    }
    .error-message {
      display: flex;
      justify-content: center;
      font-size: 30px ;
    }
    .image {
      width: 400px;
      height: 250px;
      margin: 5px;
      opacity: 0.9;
    }
    .image:hover {
      opacity: 1;
      transform: scale(1.04);
    }
    .photos {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: center;
    }
    .header {
      text-align: center;
      font-size: 20px;
    }
    .input-container {
      margin-bottom: 40px;
      display: flex;
      flex-direction: column;
      align-items: center;
    }
    .parameters {
      padding-top: 2%;
    }
    .k {
      margin-top: 1%;
    }
    .search-bar {
      width: 25%;
      height: 6vh;
      background: rgba(255, 255, 255, 0.2);
      display: flex;
      align-items: center;
      /* justify-content: space-between; */
      margin-top: 2.5vh;
      border-radius: 60px;
      padding: 10px 10px;
      /* backdrop-filter: blur(4px) saturate(180%); */
    }
    .search-bar input {
      background: transparent;
      flex: 1;
      border: 0;
      outline: none;
      font-size: 25px;
      /* padding: 24px; */
      color: white;
    }
    
    .search-bar button {
      width: 60px;
      height: 60px;
      border: 0;
      border-radius: 50%;
      background: #58629b;
      cursor: pointer;
    }
  
    .search-bar button img {
      width: 25px;
    }
  
    /* .search-button {
      flex: 2;
      width: 12%;
      height: 100%;
      background-color: rgb(183, 0, 255);
      border-radius: 10px;
      border: none;
      color: white;
    } */
    .search-button:hover {
      transform: scale(1.04);
      cursor: pointer;
    }
  </style>
  