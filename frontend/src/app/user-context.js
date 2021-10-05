import React, { useState } from 'react';

const UserContext = React.createContext([{}, () => {}]);
const UserProvider = (props) => {
  let user_last=localStorage.getItem('pat_user')
  const [user, setUser] = useState( user_last?JSON.parse(user_last) : {name:'', email:''});
  return (
    <UserContext.Provider value={[user, setUser]}>
      {props.children}
    </UserContext.Provider>
  );
}

export { UserContext, UserProvider };