import React, { useState } from 'react';
import axios from 'axios';
import Markdown from 'react-markdown';
import gfm from 'remark-gfm';

function App() {
  const [inputValue, setInputValue] = useState('');
  const [response, setResponse] = useState('');

  const handleInputChange = (e) => {
    setInputValue("Repositories that " + e.target.value);
  };

  const handleClick = async () => {
    // Replace with your server URL and endpoint
    const res = await axios.get(`/api/search?question=${inputValue}`);
    setResponse(res.data);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100vh' }}>
      <h1>Github Meta</h1>
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: '2vh' }}>
        <p style={{margin: '0', marginRight: '10px'}}>Repositories that</p>
        <input type="text" placeholder="..." style={{ padding: '10px', fontSize: '16px', width: '80vw', border: 'none',
      boxShadow: '0px 0.3vh 0.6vh 0px rgba(0,0,0,0.16)' }} onChange={handleInputChange} />
        <button onClick={handleClick} style={{ marginLeft: '1vw', borderRadius: '1.2vh' }}>Search</button>
      </div>
      {response &&
        <div style={{ width: '80%', display: 'flex', flexDirection: 'column', alignItems: 'flex-start', overflowX: 'auto' }}>
          <Markdown remarkPlugins={[gfm]} children={response} />
        </div>
      }
    </div>
  );
}

export default App;